package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"cloud.google.com/go/bigquery"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"golang.org/x/net/html/charset"
	"io"
	"log"
	"net/http"
	neturl "net/url"
	"os"
	"regexp"
	"strings"
)

type ChainedCloser struct {
	r io.Reader
	c io.Closer
}

func (c ChainedCloser) Read(p []byte) (n int, err error) { return c.r.Read(p) }
func (c ChainedCloser) Close() error                     { return c.c.Close() }

func request(method, url string, body map[string]string) (io.ReadCloser, error) {
	v := neturl.Values{}
	for key, value := range body {
		v.Set(key, value)
	}
	req, err := http.NewRequest(method, url, strings.NewReader(v.Encode()))
	if err != nil {
		log.Printf("http.NewRequest: %v", err)
		return nil, err
	}
	if len(v) != 0 {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("http.DefaultClient.Do: %v", err)
		return nil, err
	}
	if resp.StatusCode > 299 {
		return nil, fmt.Errorf("Response failed with status code: %d and\nbody: %s\n", resp.StatusCode, body)
	}

	return resp.Body, nil
}

func convert(label string, r io.ReadCloser) (io.ReadCloser, error) {
	nr, err := charset.NewReaderLabel(label, r)
	if err != nil {
		return nil, err
	}
	return ChainedCloser{nr, r}, nil
}

func unzip(reader io.ReadCloser) (io.ReadCloser, error) {
	b := bytes.NewBuffer([]byte{})
	size, err := io.Copy(b, reader)
	if err != nil {
		return nil, err
	}
	reader.Close()

	br := bytes.NewReader(b.Bytes())
	r, err := zip.NewReader(br, size)
	if err != nil {
		return nil, err
	}

	if len(r.File) == 0 {
		return nil, nil
	}
	return r.File[0].Open()
}

func csvHeader(r io.Reader) ([]string, io.Reader, error) {
	br := bufio.NewReader(r)
	bom, err := br.Peek(3)
	if err != nil {
		return nil, nil, err
	}
	// UTF-8 with BOM
	if bom[0] == 0xEF && bom[1] == 0xBB && bom[2] == 0xBF {
		br.Discard(3)
	}
	rr := csv.NewReader(br)
	rr.LazyQuotes = true
	header, err := rr.Read()
	if err != nil {
		return nil, nil, err
	}
	return header, br, nil
}

func load(projectID string, datasetID string, tableID string, r io.ReadCloser) error {
	header, br, err := csvHeader(r)
	defer r.Close()

	if err != nil {
		return err
	}

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return err
	}
	rs := bigquery.NewReaderSource(br)
	rs.AllowQuotedNewlines = true

	schema := make([]*bigquery.FieldSchema, len(header))
	var invalidCharacters = regexp.MustCompile(`[^\p{L}\p{N}\p{Pc}\p{Pd}\p{M}&%=+:'<>#|]`)
	for i, v := range header {
		name := invalidCharacters.ReplaceAllString(v, "_")
		schema[i] = &bigquery.FieldSchema{Name: name, Type: bigquery.StringFieldType}
	}
	rs.Schema = schema

	ds := client.Dataset(datasetID)
	loader := ds.Table(tableID).LoaderFrom(rs)
	loader.WriteDisposition = bigquery.WriteTruncate
	job, err := loader.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}
	if status.Err() != nil {
		return status.Err()
	}
	return nil
}

func main() {
	log.Print("starting server...")
	http.HandleFunc("/", handler)

	// Determine port for HTTP service.
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
		log.Printf("defaulting to port %s", port)
	}

	// Start HTTP server.
	log.Printf("listening on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}

func (t Tweak) tweak(reader io.ReadCloser) (io.ReadCloser, error) {
	var nextReader io.ReadCloser
	var err error

	switch t.Call {
	case "unzip":
		nextReader, err = unzip(reader)
	case "convert":
		nextReader, err = convert(t.Args["charset"], reader)
	default:
		return nil, fmt.Errorf("unsupported call: %s", t.Call)
	}

	if err != nil {
		log.Printf("call transformers: %v", err)
		return nil, err
	}
	return nextReader, nil
}

type Extraction struct {
	Method string
	Url    string
	Body   map[string]string
}
type Tweak struct {
	Call string
	Args map[string]string
}
type Loading struct {
	ProjectID string
	DatasetID string
	TableID   string
}

func handler(w http.ResponseWriter, r *http.Request) {
	var d struct {
		PreExtraction PreExtraction
		Extraction    Extraction
		Tweaks        []Tweak
		Loading       Loading
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewDecoder(r.Body).Decode(&d); err != nil {
		log.Printf("json.NewDecoder: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, `{"error": "Internal Server Error"}`)
		return
	}
	e, err := d.PreExtraction.preExtract(d.Extraction)
	if err != nil {
		log.Printf("preExtract: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, `{"error": "Internal Server Error"}`)
		return
	}
	d.Extraction = e
	reader, err := request(d.Extraction.Method, d.Extraction.Url, d.Extraction.Body)
	fmt.Printf("%v", d.Extraction.Body)
	if err != nil {
		log.Printf("request: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, `{"error": "Internal Server Error"}`)
		return
	}

	for _, t := range d.Tweaks {
		reader, err = t.tweak(reader)
		if err != nil {
			log.Printf("tweak: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintln(w, `{"error": "Internal Server Error"}`)
			return
		}
	}

	if err := load(d.Loading.ProjectID, d.Loading.DatasetID, d.Loading.TableID, reader); err != nil {
		log.Printf("load: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, `{"error": "Internal Server Error"}`)
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "{}")
}

func formatMap(templates map[string]string, pattern *regexp.Regexp, content string) map[string]string {
	matches := pattern.FindAllStringSubmatchIndex(content, -1)

	m := make(map[string]string)
	for key := range templates {
		template := templates[key]
		var result []byte
		for _, submatches := range matches {
			result = pattern.ExpandString(result, template, content, submatches)
		}
		m[key] = string(result)
	}
	return m
}

type PreExtraction struct {
	Method  string
	Url     string
	Body    map[string]string
	Pattern string
}

func (p PreExtraction) preExtract(e Extraction) (Extraction, error) {
	if p.Method == "" && p.Url == "" {
		return e, nil
	}
	reader, err := request(p.Method, p.Url, p.Body)
	if err != nil {
		return e, err
	}
	defer reader.Close()

	b, err := io.ReadAll(reader)
	if err != nil {
		log.Fatal(err)
	}
	content := string(b)
	pattern := regexp.MustCompile(p.Pattern)

	body := formatMap(e.Body, pattern, content)

	return Extraction{
		e.Method,
		e.Url,
		body,
	}, nil
}

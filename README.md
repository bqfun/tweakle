# tweakle
Extract/Transform/Load処理を行うCloud Run用のウェブサービスの実装。

特に以下のことを行う。
- Extract: HTTPリクエスト
- Transform: 単純な加工（tweak）
- Load: Cloud Storageへのアップロード

## ビルド方法
buildpacksを用いてビルドする。

```shell
pack build tweakle --builder gcr.io/buildpacks/builder
```

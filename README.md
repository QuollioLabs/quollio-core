# quollio-core

## Description (説明)

This Python library collects advanced metadata like table to table lineage or anomaly record and ingests them to QDC.

このPythonライブラリは、テーブル間のリネージやデータの統計値などのメタデータを取得し、データカタログのアセットに反映します。


## Prerequisite (前提条件)
Before you begin to use this, you need to do the following.
- Add your assets to QDC with metadata agent.
- Issue client id and client secret on QDC for External API.

このシステムを使用する前に、以下の手順を実行する必要があります。
- Metadata Agentを使用して、データカタログにアセットを登録する。
- 外部API用の、データカタログ上で認証に必要なクライアントIDとシークレットを発行する。

## Install (インストール)

Install with the following command.

下記のコマンドでインストールしてください。

```
$ pip install quollio-core
```

To see available commands and options, please run the following command. (ex: Snowflake)

コマンドやオプションの詳細については、下記のコマンドを実行してください。(例: Snowflake)

```
$ python -m quollio_core.snowflake -h
```

Then run commands with the options provided.

その後、オプションを指定してコマンドを実行してください。

| Command (コマンド) | Description (概要)                                                                                       |
| ------------------ | -------------------------------------------------------------------------------------------------------- |
| build_view         | Build views for lineage and statistics.<br>リネージと統計情報を生成するビューを作成します。              |
| load_lineage       | Load lineage from created views to Quollio.<br>作成したビューからリネージデータをQuollioにロードします。 |
| load_stats         | Load statistics from created views to Quollio.<br>作成したビューから統計情報をQuollioにロードします。    |


## Development (開発)

### Install (インストール)

Create `.env` file in the root level of repository and set the following environment variables.

リポジトリのルートレベルに`.env`ファイルを作成し、下記の環境変数を設定してください。

```
AWS_REGION=[AWS region]
IMAGE_NAME=[Container image name you want to use]
QUOLLIO_CORE_VERSION=[The quollio core version you use]
```

To install local packages, run the following command.

ローカルパッケージをインストールするには、下記のコマンドを実行してください。

```
$ make install
```

### Build (ビルド)

To build Docker image with local files, run the following command.

ローカルファイルでDocker imageをビルドするには、下記のコマンドを実行してください。

```
$ make build-local
```

### Unit test (ユニットテスト)

To run unit tests, run the following command.

ユニットテストを実行するには、下記のコマンドを実行してください。

```
$ make test
```

### Docs (ドキュメント)

To auto generate docs for dbt, run the following command. (ex. Snowflake)

dbtのドキュメントを自動生成するには、下記のコマンドを実行してください。(例: Snowflake)

```
$ cd quollio_core/dbt_projects/snowflake
$ dbt-osmosis yaml refactor \
--force-inheritance \
--project-dir ./ \
--profiles-dir ./profiles \
--vars '{query_role: <snowflake role>, sample_method: SAMPLE(10)}'
```

### Push (プッシュ)

The push command in `Makefile` is for pushing the image to ECR. If you want to push the image to other container registry, please change the command.

`Makefile`のpushコマンドは、ECRにイメージをプッシュするためのものです。他のコンテナレジストリにイメージをプッシュする場合は、コマンドを変更してください。

## License (ライセンス)

This library is licensed under the AGPL-3.0 License, but the dependencies are not licensed under the AGPL-3.0 License but under their own licenses. You may change the source code of the dependencies within the scope of their own licenses. Please refer to `pyproject.toml` for the dependencies.

このライブラリはAGPL-3.0ライセンスで保護されていますが、依存関係はAGPL-3.0ライセンスではなく、それぞれのライセンスで保護されています。依存関係のソースコードは、それぞれのライセンスの範囲内で変更することができます。依存関係については、`pyproject.toml`を参照してください。

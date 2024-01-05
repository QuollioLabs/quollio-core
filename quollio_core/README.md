# quollio-core (English)

## Description
This system collects advanced metadata like table to table lineage or anomaly record and ingests them to QDC.


## Prerequisite
Before you begin to use this, you need to do the following.
- Add your assets to QDC with metadata agent.
- Issue External API client id and client secret on QDC.

## Install
Install with the following command.
`pip install quollio-core`

## Usage
Here is an example of creating a view for snowflake lineage. Please enter any values for <>.
```
from quollio_core.repository.qdc import QDCExternalAPIClient
from quollio_core.repository.snowflake import SnowflakeConnectionConfig
from quollio_core.snowflake_lineage_profiler import execute


def view_build_only():
    company_id = "<company id issued by quollio.>"
    build_view_connection = SnowflakeConnectionConfig(
            account_id="<Snowflake account id. Please use the same id of metadata agent.>",
            account_role="<Role necessary for creating view in your account>",
            account_user="<user name>",
            account_password="<password>",
            account_warehouse="<compute warehouse>",
    )
    qdc_client = QDCExternalAPIClient(
        client_id="<client id issued on QDC.>",
        client_secret="<client secret>",
        base_url="<base endpoint>",
    )
    execute(
        company_id=company_id,
        sf_build_view_connections=build_view_connection,
        qdc_client=qdc_client,
        is_view_build_only=True,
    )

if __name__ == "__main__":
    view_build_only()
```
Please refer to the scripts in `./examples` for other usages.


## Development
### How to test
#### Unittest
1. Run `make test`

## License

This library is licensed under the AGPL-3.0 License, but the dependencies are not licensed under the AGPL-3.0 License but under their own licenses. You may change the source code of the dependencies within the scope of their own licenses. Please refer to `pyproject.toml` for the dependencies.

# quollio-core (日本語)

## 説明
このシステムは、テーブル間のリネージやデータの統計値などのメタデータを取得し、データカタログのアセットに反映します。


## 前提条件
このシステムを使用する前に、以下の手順を実行する必要があります。
- Metadata Agentを利用して、データカタログにアセットを登録する。
- [ExternalAPI](https://api.docs.quollio.com/#section/Authorization)を利用するため、データカタログ上で認証に必要なクライアントIDとシークレットを発行する。


## インストール
下記のコマンドでインストールしてください。
`pip install quollio-core`

## 利用方法
ビューを作成する例を記載します。`<>`の値に任意の値を入力してください。
```
from quollio_core.repository.qdc import QDCExternalAPIClient
from quollio_core.repository.snowflake import SnowflakeConnectionConfig
from quollio_core.snowflake_lineage_profiler import execute


def view_build_only():
    company_id = "<Quollioから提供される会社ID>"
    build_view_connection = SnowflakeConnectionConfig(
            account_id="<Snowflakeのアカウント名。MetadataAgentで使用しているものと同じものをご利用ください>",
            account_role="<ビューの作成に必要なロール>",
            account_user="<ユーザー名>",
            account_password="<パスワード>",
            account_warehouse="<ウェアハウス>",
    )
    qdc_client = QDCExternalAPIClient(
        client_id="<QDCで発行したクライアントID>",
        client_secret="<クライアントシークレット>",
        base_url="<APIのベースエンドポイント>",
    )
    execute(
        company_id=company_id,
        sf_build_view_connections=build_view_connection,
        qdc_client=qdc_client,
        is_view_build_only=True,
    )

if __name__ == "__main__":
    view_build_only()
```
その他の利用方法については、`./examples`以下のスクリプトを参考にしてください。


## 開発
### テスト方法
#### ユニットテスト
1. `make test`を実行

## ライセンス

このライブラリはAGPL-3.0ライセンスで保護されていますが、依存関係はAGPL-3.0ライセンスではなく、それぞれのライセンスで保護されています。依存関係のソースコードは、それぞれのライセンスの範囲内で変更することができます。依存関係については、`pyproject.toml`を参照してください。

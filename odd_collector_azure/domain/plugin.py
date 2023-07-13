from typing import Literal, Optional
from pydantic import SecretStr

from odd_collector_sdk.domain.plugin import Plugin
from odd_collector_sdk.types import PluginFactory
from odd_collector_azure.adapters.blob_storage.dataset_config import DatasetConfig


class AzurePlugin(Plugin):
    client_id: str  # client_id of registered in AD app
    client_secret: str  # client secret of registered in AD app
    username: str
    password: str
    domain: str  # yourdomain.com


class PowerBiPlugin(AzurePlugin):
    type: Literal["powerbi"]


class AzureSQLPlugin(Plugin):
    type: Literal["azure_sql"]
    database: str
    server: str
    port: str
    username: str
    password: str
    encrypt: str = "yes"
    trust_server_certificate: str = "no"
    connection_timeout: str = "30"


class BlobPlugin(Plugin):
    type: Literal["blob_storage"]
    account_name: str
    account_key: Optional[SecretStr]
    connection_string: Optional[SecretStr]
    datasets: list[DatasetConfig]


PLUGIN_FACTORY: PluginFactory = {
    "powerbi": PowerBiPlugin,
    "azure_sql": AzureSQLPlugin,
    "blob_storage": BlobPlugin,
}

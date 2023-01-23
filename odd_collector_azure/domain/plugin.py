from typing import Literal
from odd_collector_sdk.domain.plugin import Plugin
from odd_collector_sdk.types import PluginFactory


class AzureSQL(Plugin):
    database: str
    server: str
    port: str
    username: str
    password: str


class AzurePlugin(Plugin):
    client_id: str  # client_id of registered in AD app
    client_secret: str  # client secret of registered in AD app
    username: str
    password: str
    domain: str  # yourdomain.com


class PowerBiPlugin(AzurePlugin):
    type: Literal["powerbi"]


class AzureSQLPlugin(AzureSQL):
    type: Literal["azure_sql"]


PLUGIN_FACTORY: PluginFactory = {
    "powerbi": PowerBiPlugin,
    "azure_sql": AzureSQLPlugin
}

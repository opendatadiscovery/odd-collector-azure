from typing import Literal
from odd_collector_sdk.domain.plugin import Plugin
from odd_collector_sdk.types import PluginFactory


class AzurePlugin(Plugin):
    client_id: str  # client_id of registered in AD app
    client_secret: str  # client secret of registered in AD app
    username: str
    password: str
    domain: str  # yourdomain.com


class PowerBiPlugin(AzurePlugin):
    type: Literal["powerbi"]


PLUGIN_FACTORY: PluginFactory = {
    "powerbi": PowerBiPlugin
}

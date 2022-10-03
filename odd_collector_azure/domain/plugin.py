from typing import Literal
from odd_collector_sdk.domain.plugin import Plugin
from odd_collector_sdk.types import PluginFactory


class AzurePlugin(Plugin):
    client_id: str
    client_secret: str
    username: str
    password: str
    resource: str


class PowerBiPlugin(AzurePlugin):
    type: Literal["powerbi"]


PLUGIN_FACTORY: PluginFactory = {
    "powerbi": PowerBiPlugin
}

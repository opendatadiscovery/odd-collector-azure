import asyncio
import logging
import os
from pathlib import Path

from odd_collector_sdk.collector import Collector

from odd_collector_azure.domain.plugin import PLUGIN_FACTORY

logging.basicConfig(
    level=os.getenv("LOGLEVEL", "INFO"),
    format="[%(asctime)s] %(levelname)s in %(name)s: %(message)s",
)
logger = logging.getLogger("odd-collector-azure")


try:

    loop = asyncio.get_event_loop()

    config_path = Path().cwd() / os.getenv("CONFIG_PATH", "collector_config.yaml")

    root_package = "odd_collector_azure.adapters"

    collector = Collector(
        config_path=str(config_path),
        root_package=root_package,
        plugin_factory=PLUGIN_FACTORY,
    )

    loop.run_until_complete(collector.register_data_sources())

    collector.start_polling()

    asyncio.get_event_loop().run_forever()
except Exception as e:
    logger.error(e, exc_info=True)
    asyncio.get_event_loop().stop()

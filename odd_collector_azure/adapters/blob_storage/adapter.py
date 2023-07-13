import traceback as tb
from typing import Iterable, Union

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntityList
from oddrn_generator.generators import Generator

from odd_collector_azure.adapters.blob_storage.blob_generator import BlobGenerator
from odd_collector_azure.adapters.blob_storage.file_system import FileSystem
from odd_collector_azure.adapters.blob_storage.mapper.container import map_container
from odd_collector_azure.domain.plugin import BlobPlugin
from .logger import logger


class Adapter(AbstractAdapter):
    config: BlobPlugin
    generator: Union[Generator, BlobGenerator]

    def __init__(self, config: BlobPlugin):
        self.config = config
        self.generator = BlobGenerator()
        self.fs = FileSystem(config)

    def get_data_source_oddrn(self) -> str:
        return self.generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> Iterable[DataEntityList]:
        for dataset_config in self.config.datasets:
            try:
                container = self.fs.get_container(dataset_config)
                data_entities = map_container(
                    self.config.account_name, container, self.generator
                )

                yield DataEntityList(
                    data_source_oddrn=self.get_data_source_oddrn(),
                    items=list(data_entities),
                )
            except Exception as e:
                logger.error(
                    f"Error while processing container {dataset_config.containers}: {e}."
                    " SKIPPING."
                )
                logger.debug(tb.format_exc())
                continue

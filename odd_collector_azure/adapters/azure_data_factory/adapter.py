from collections import defaultdict
from typing import Iterable, Union

from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_models import DataEntity
from odd_models.models import DataEntityList
from oddrn_generator import AzureDataFactoryGenerator
from oddrn_generator.generators import Generator
from odd_collector_azure.domain.plugin import DataFactoryPlugin
from .client import DataFactoryClient
from .domain import ADFActivity
from .logger import logger
from .mapper.factory import map_factory
from .mapper.activity import map_activity
from .mapper.pipeline import map_pipeline
from functools import partial


class Adapter(BaseAdapter):
    config: DataFactoryPlugin
    generator: Union[Generator, AzureDataFactoryGenerator]

    def __init__(self, config: DataFactoryPlugin):
        self.client = DataFactoryClient(config)
        super().__init__(config)

    def create_generator(self) -> Generator:
        return AzureDataFactoryGenerator(
            azure_cloud_settings={
                'domain': self.config.subscription,
            }
        )

    def get_data_entity_list(self) -> DataEntityList:
        activities_entities: list[DataEntity] = []
        pipelines_entities: list[DataEntity] = []
        try:
            self.generator.set_oddrn_paths(factories=self.config.factory)
            factory = self.client.get_factory()
            pipelines = self.client.get_pipelines(factory.name)
            for pipeline in pipelines:
                self.generator.set_oddrn_paths(pipelines=pipeline.name)
                activities = [ADFActivity(act) for act in pipeline.activities]
                activities_entities_tmp = [map_activity(self.generator, activity) for activity in activities]
                pipelines_entities.append(map_pipeline(self.generator, pipeline, activities_entities_tmp))
                activities_entities.extend(activities_entities_tmp)

            factory_entity = map_factory(self.generator, factory, pipelines_entities)

            return DataEntityList(
                data_source_oddrn=self.get_data_source_oddrn(),
                items=[*activities_entities, *pipelines_entities, factory_entity],
            )
        except Exception as e:
            logger.error(
                f"Error while processing: {e}."
                " SKIPPING.", exc_info=True
            )

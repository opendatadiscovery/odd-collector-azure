from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntityList
from typing import Type, Optional
from odd_collector_azure.domain.plugin import PowerBiPlugin
from .client import PowerBiClient
from oddrn_generator.generators import Generator, HostnameModel
from oddrn_generator.path_models import BasePathsModel
from .domain.dataset import Dataset
from odd_models.models import DataEntity, DataEntityType, DataEntityGroup


class PowerBiPathModel(BasePathsModel):
    datasources: Optional[str]
    datasets: Optional[str]
    dashboards: Optional[str]

    class Config:
        dependencies_map = {
            "datasets": ("datasets",),
            "dashboards": ("dashboards",),
        }


class PowerBiGenerator(Generator):
    source = "powerbi"
    paths_model = PowerBiPathModel
    server_model = HostnameModel


def map_dataset(
        oddrn_generator: PowerBiGenerator,
        dataset: Dataset,
) -> DataEntity:
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("datasets", dataset.name),
        name=dataset.name,
        type=DataEntityType.DATABASE_SERVICE,
        metadata=[],
        data_entity_group=DataEntityGroup(
            entities_list=dataset.datasources
        ),
    )


class Adapter(AbstractAdapter):
    def __init__(
            self, config: PowerBiPlugin, client: Type[PowerBiPlugin] = None
    ) -> None:
        client = client or PowerBiClient
        self.client = client(config)

        self.__oddrn_generator = PowerBiGenerator(
            host_settings=config.resource
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    async def get_data_entity_list(self) -> DataEntityList:
        datasets = await self.client.get_datasets()
        enriched_datasets = await self.client.enrich_datasets_with_datasources_oddrns(datasets)
        datasets_entities = [map_dataset(self.__oddrn_generator, dataset) for dataset in enriched_datasets]
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[*datasets_entities],
        )

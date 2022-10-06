from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntityList
from typing import Type, Optional, List, Dict
from odd_collector_azure.domain.plugin import PowerBiPlugin
from .client import PowerBiClient
from oddrn_generator.generators import Generator, HostnameModel
from oddrn_generator.path_models import BasePathsModel
from .domain.dataset import Dataset
from .domain.dashboard import Dashboard
from odd_models.models import DataEntity, DataEntityType, DataConsumer, MetadataExtension


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


_METADATA_SCHEMA_URL_PREFIX: str = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/"
    "extensions/powerbi.json#/definitions/PowerBi"
)


def map_dataset(
        oddrn_generator: PowerBiGenerator,
        dataset: Dataset,
) -> DataEntity:
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("datasets", dataset.name),
        name=dataset.name,
        type=DataEntityType.DATABASE_SERVICE,
        metadata=[MetadataExtension(
            schema_url=_METADATA_SCHEMA_URL_PREFIX,
            metadata={
                "id": dataset.id
            },
        )],
        owners=dataset.owner,
        data_consumer=DataConsumer(
            inputs=dataset.datasources
        ),
    )


def map_dashboard(
        oddrn_generator: PowerBiGenerator,
        dashboard: Dashboard,
        datasets_ids: List[str],
        datasets_oddrns_map: Dict[str, str]
) -> DataEntity:
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("dashboards", dashboard.display_name),
        name=dashboard.display_name,
        type=DataEntityType.DASHBOARD,
        metadata=[],
        data_consumer=DataConsumer(
            inputs=[dataset_oddrn for dataset_id, dataset_oddrn in datasets_oddrns_map.items() if
                    dataset_id in datasets_ids]
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

    @staticmethod
    def __create_dataset_id_oddrn_map(datasets_entities: List[DataEntity]):
        return {dataset_entity.metadata[0].metadata['id']: dataset_entity.oddrn for dataset_entity in datasets_entities}

    async def get_data_entity_list(self) -> DataEntityList:
        dashboards = await self.client.get_dashboards()
        dashboard_datasets_map = await self.client.get_datasets_ids_for_dashboards(
            [dashboard.id for dashboard in dashboards])
        datasets = await self.client.get_datasets()
        enriched_datasets = await self.client.enrich_datasets_with_datasources_oddrns(datasets)
        datasets_entities = [map_dataset(self.__oddrn_generator, dataset) for dataset in enriched_datasets]
        datasets_oddrns_map = self.__create_dataset_id_oddrn_map(datasets_entities)
        dashboards_entities = [
            map_dashboard(self.__oddrn_generator, dashboard, dashboard_datasets_map[dashboard.id], datasets_oddrns_map)
            for dashboard in dashboards]
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[*datasets_entities, *dashboards_entities],
        )

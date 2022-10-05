from odd_collector_azure.azure.azure_client import AzureClient, RequestArgs
from odd_collector_azure.domain.plugin import PowerBiPlugin
from aiohttp import ClientSession
from typing import Dict, Any, List
from odd_models.models import DataEntity
from .domain.dataset import Dataset
from .mappers.datasources import datasources_factory


class PowerBiClient:
    def __init__(self, config: PowerBiPlugin):
        self.__client = AzureClient(config)
        self.__base_url = 'https://api.powerbi.com/v1.0/myorg/'

    async def __get_nodes(self, endpoint: str, params: Dict[str, Any] = None) -> dict:
        async with ClientSession() as session:
            response = await self.__client.fetch_async_response(
                session,
                RequestArgs(
                    method="GET",
                    url=self.__base_url + endpoint,
                    headers=await self.__client.build_headers(),
                    params=params,
                ),
            )
            return response['value']

    async def get_datasets(self) -> List[Dataset]:
        datasets_nodes = await self.__get_nodes('datasets')
        return [Dataset(id=datasets_node.get('id'),
                        name=datasets_node.get('name'),
                        owner=datasets_node.get('configuredBy')
                        ) for datasets_node in datasets_nodes]

    async def __get_datasources_entities_for_dataset(self, dataset_id: str) -> List[DataEntity]:
        datasources_nodes = await self.__get_nodes(f'datasets/{dataset_id}/datasources')
        datasources_entities: List[DataEntity] = []
        for datasource_node in datasources_nodes:
            datasource_type = datasource_node['datasourceType']
            datasource_engine = datasources_factory.get(datasource_type)
            datasource_entity = datasource_engine(datasource_node).map_database()
            datasources_entities.append(datasource_entity)
        return datasources_entities

    async def enrich_datasets_with_datasources_oddrns(self, datasets: List[Dataset]) -> List[Dataset]:
        enriched_datasets: List[Dataset] = []
        for dataset in datasets:
            datasources = await self.__get_datasources_entities_for_dataset(dataset.id)
            dataset.datasources = [datasource.oddrn for datasource in datasources]
            enriched_datasets.append(dataset)

        return enriched_datasets

    async def get_dashboards(self):
        dashboards_nodes = await self.__get_nodes('dashboards/216cfe89-0727-46f1-9864-f0f23c6af720/tiles')
        return dashboards_nodes

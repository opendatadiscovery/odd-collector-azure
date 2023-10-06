from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient

from odd_collector_azure.domain.plugin import DataFactoryPlugin

from .domain import ADFPipeline, DataFactory


class DataFactoryClient:
    def __init__(self, config: DataFactoryPlugin):
        self.client = DataFactoryManagementClient(
            credential=DefaultAzureCredential(),
            subscription_id="98e9f5da-db8f-4aa2-b8ab-4e71c999db01",
        )
        self.resource_group = config.resource_group
        self.factory = config.factory

    def get_pipelines(self, factory: str) -> list[ADFPipeline]:
        pipeline_resources = self.client.pipelines.list_by_factory(
            resource_group_name=self.resource_group, factory_name=factory
        )

        return [ADFPipeline(pipeline) for pipeline in pipeline_resources]

    def get_factory(self) -> DataFactory:
        factory_resource = self.client.factories.get(
            resource_group_name=self.resource_group,
            factory_name=self.factory,
        )

        return DataFactory(factory_resource)

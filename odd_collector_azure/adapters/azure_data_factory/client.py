from collections import defaultdict
from datetime import datetime

from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import PipelineResource
from odd_collector_sdk.domain.filter import Filter

from odd_collector_azure.domain.plugin import DataFactoryPlugin

from .domain import ADFActivityRun, ADFPipeline, ADFPipelineRun, DataFactory


class DataFactoryClient:
    def __init__(self, config: DataFactoryPlugin):
        self.client = DataFactoryManagementClient(
            credential=DefaultAzureCredential(),
            subscription_id="98e9f5da-db8f-4aa2-b8ab-4e71c999db01",
        )
        self.resource_group = config.resource_group
        self.factory = config.factory

    def get_pipelines(self, factory: str, filter_: Filter) -> list[ADFPipeline]:
        pipeline_resources: list[
            PipelineResource
        ] = self.client.pipelines.list_by_factory(
            resource_group_name=self.resource_group, factory_name=factory
        )

        return [
            ADFPipeline(pipeline)
            for pipeline in pipeline_resources
            if filter_.is_allowed(pipeline.name)
        ]

    def get_factory(self) -> DataFactory:
        factory_resource = self.client.factories.get(
            resource_group_name=self.resource_group,
            factory_name=self.factory,
        )

        return DataFactory(factory_resource)

    def get_activity_runs(
        self, pipeline_name: str
    ) -> defaultdict[str, list[ADFActivityRun]]:
        start_timestamp = "2010-01-01T00:00:00.0000000Z"
        activity_runs = defaultdict(list)
        pipeline_runs = self.client.pipeline_runs.query_by_factory(
            resource_group_name=self.resource_group,
            factory_name=self.factory,
            filter_parameters={
                "filters": [
                    {
                        "operand": "PipelineName",
                        "operator": "Equals",
                        "values": [pipeline_name],
                    }
                ],
                "lastUpdatedAfter": start_timestamp,
                "lastUpdatedBefore": datetime.now(),
            },
        ).value
        for pipeline_run in pipeline_runs:
            runs = self.client.activity_runs.query_by_pipeline_run(
                resource_group_name=self.resource_group,
                factory_name=self.factory,
                run_id=pipeline_run.run_id,
                filter_parameters={
                    "lastUpdatedAfter": start_timestamp,
                    "lastUpdatedBefore": datetime.now(),
                },
            ).value
            for run in runs:
                activity_runs[run.activity_name].append(ADFActivityRun(run))

        return activity_runs

    def get_pipeline_runs(self, pipeline_name: str) -> list[ADFPipelineRun]:
        start_timestamp = "2010-01-01T00:00:00.0000000Z"
        pipeline_runs = defaultdict(list)
        runs = self.client.pipeline_runs.query_by_factory(
            resource_group_name=self.resource_group,
            factory_name=self.factory,
            filter_parameters={
                "filters": [
                    {
                        "operand": "PipelineName",
                        "operator": "Equals",
                        "values": [pipeline_name],
                    }
                ],
                "lastUpdatedAfter": start_timestamp,
                "lastUpdatedBefore": datetime.now(),
            },
        ).value

        # for run in runs:
        #     pipeline_runs[run.pipeline_name].append(ADFPipelineRun(run))

        return [ADFPipelineRun(run) for run in runs]

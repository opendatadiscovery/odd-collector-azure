from dataclasses import dataclass

from azure.mgmt.datafactory.models import PipelineResource, Factory, Activity, Resource
from funcy import omit
from odd_collector_sdk.utils.metadata import HasMetadata


class MetadataMixin:
    resource: Resource
    excluded_properties = ()

    @property
    def name(self) -> str:
        return self.resource.name

    @property
    def odd_metadata(self) -> dict:
        return omit(self.resource.__dict__, self.excluded_properties)


@dataclass
class DataFactory(MetadataMixin, HasMetadata):
    resource: Factory
    excluded_properties = ("name",)


@dataclass
class ADFPipeline(MetadataMixin, HasMetadata):
    resource: PipelineResource
    # excluded_properties = ("name", "activities", "parameters", "policy")
    excluded_properties = ("name",)

    @property
    def activities(self) -> list[Activity]:
        return self.resource.activities


@dataclass
class ADFActivity(MetadataMixin, HasMetadata):
    resource: Activity
    # excluded_properties = ("name", "policy", "inputs", "outputs", "typeProperties", "dataset", "store_settings")
    excluded_properties = ("name",)

from dataclasses import dataclass

from azure.mgmt.datafactory.models import Activity, Factory, PipelineResource, Resource
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
    excluded_properties = ("name",)

    @property
    def activities(self) -> list[Activity]:
        return self.resource.activities


@dataclass
class ADFActivity(MetadataMixin, HasMetadata):
    resource: Activity
    excluded_properties = ("name",)

    @property
    def depends_on(self):
        return self.resource.depends_on

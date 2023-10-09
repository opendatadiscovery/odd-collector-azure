from collections import defaultdict
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
    all_activities: list[Activity]
    excluded_properties = ("name",)

    @property
    def inputs(self) -> list[str]:
        return [dep.activity for dep in self.resource.depends_on]

    @property
    def outputs(self) -> list[str]:
        dependency_map = self._build_dependency_map()
        return dependency_map.get(self.resource.name, [])

    def _build_dependency_map(self):
        dependency_map = defaultdict(list)
        for activity in self.all_activities:
            if activity.depends_on:
                for dependency in activity.depends_on:
                    dependency_map[dependency.activity].append(activity.name)
        return dependency_map

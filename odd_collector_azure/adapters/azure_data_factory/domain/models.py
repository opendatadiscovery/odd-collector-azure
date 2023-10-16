from collections import defaultdict
from dataclasses import dataclass, field

from azure.mgmt.datafactory.models import (
    Activity,
    ActivityRun,
    Factory,
    PipelineResource,
    PipelineRun,
    Resource,
)
from funcy import omit
from odd_collector_sdk.utils.metadata import HasMetadata
from odd_models import JobRunStatus


class MetadataMixin:
    resource: Resource
    excluded_properties = ("name",)

    @property
    def name(self) -> str:
        return self.resource.name

    @property
    def odd_metadata(self) -> dict:
        return omit(self.resource.__dict__, self.excluded_properties)


@dataclass
class DataFactory(MetadataMixin, HasMetadata):
    resource: Factory


@dataclass
class ADFPipeline(MetadataMixin, HasMetadata):
    resource: PipelineResource

    @property
    def activities(self) -> list[Activity]:
        return self.resource.activities


@dataclass
class ADFActivity(MetadataMixin, HasMetadata):
    resource: Activity
    all_activities: list[Activity] = field(default_factory=list)

    @property
    def inputs(self) -> list[str]:
        return [dep.activity for dep in self.resource.depends_on]

    @property
    def outputs(self) -> list[str]:
        dependency_map = self._build_dependency_map()
        return dependency_map.get(self.resource.name, [])

    @property
    def activities(self):
        activities = (
            self.resource.activities if hasattr(self.resource, "activities") else None
        )
        return activities

    def _build_dependency_map(self):
        dependency_map = defaultdict(list)
        for activity in self.all_activities:
            if activity.depends_on:
                for dependency in activity.depends_on:
                    dependency_map[dependency.activity].append(activity.name)
        return dependency_map


@dataclass
class ADFPipelineRun(MetadataMixin, HasMetadata):
    resource: PipelineRun

    @property
    def id(self):
        return self.resource.run_id

    @property
    def pipeline_name(self):
        return self.resource.pipeline_name

    @property
    def start_time(self):
        return self.resource.run_start

    @property
    def end_time(self):
        return self.resource.run_end

    @property
    def status(self):
        return (
            JobRunStatus.SUCCESS
            if self.resource.status == "Succeeded"
            else JobRunStatus.FAILED
        )


@dataclass
class ADFActivityRun(MetadataMixin, HasMetadata):
    resource: ActivityRun

    @property
    def id(self):
        return self.resource.activity_run_id

    @property
    def activity_name(self):
        return self.resource.activity_name

    @property
    def start_time(self):
        return self.resource.activity_run_start

    @property
    def end_time(self):
        return self.resource.activity_run_end

    @property
    def status(self):
        return (
            JobRunStatus.SUCCESS
            if self.resource.status == "Succeeded"
            else JobRunStatus.FAILED
        )

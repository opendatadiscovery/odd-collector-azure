from typing import Optional

from odd_collector_sdk.domain.filter import Filter
from pydantic import BaseModel


class FolderAsDataset(BaseModel):
    """
    Configuration for folder as dataset.
    If folder is a dataset, then all files in the folder will be treated as a single dataset.
    """

    file_format: str
    flavor: str = None
    field_names: Optional[list[str]] = None


class DatasetConfig(BaseModel):
    containers: str
    prefix: Optional[str]
    folder_as_dataset: Optional[FolderAsDataset] = None
    file_filter: Optional[Filter] = Filter()

    @property
    def full_path(self) -> str:
        containers = self.containers.strip("/")
        prefix = self.prefix

        return f"{containers}/{prefix.strip('/')}" if prefix else containers

    def allow(self, name: str) -> bool:
        return self.file_filter.is_allowed(name)

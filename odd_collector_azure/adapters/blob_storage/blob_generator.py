from typing import Optional

from oddrn_generator import Generator
from oddrn_generator.path_models import BasePathsModel
from oddrn_generator.server_models import (
    AbstractServerModel,
)
from pydantic import BaseModel


class BlobPathsModel(BasePathsModel):
    storages: Optional[str]
    containers: Optional[str]
    keys: Optional[str]
    columns: Optional[str]
    class Config:
        dependencies_map = {
            "storages": ("storages",),
            "containers": ("storages", "containers"),
            "keys": ("storages", "containers", "keys"),
            "columns": ("storages", "containers", "keys", "columns"),
        }


class BlobModel(AbstractServerModel, BaseModel):
    """Container name is unique across Azure"""

    def __str__(self) -> str:
        return "cloud/azure"

    @classmethod
    def create(cls, config):
        return cls()


class BlobGenerator(Generator):
    source = "blob_storage"
    paths_model = BlobPathsModel
    server_model = BlobModel

from typing import Optional

from oddrn_generator import Generator
from oddrn_generator.path_models import BasePathsModel
from oddrn_generator.server_models import HostnameModel
from pydantic import Field


class AzureSQLPathsModel(BasePathsModel):
    databases: str
    tables: Optional[str]
    views: Optional[str]
    columns: Optional[str]
    tables_columns: Optional[str] = Field(alias="columns")
    views_columns: Optional[str] = Field(alias="columns")

    class Config:
        dependencies_map = {
            "databases": ("databases",),
            "tables": ("databases", "tables"),
            "views": ("databases", "views"),
            "tables_columns": ("databases", "tables", "tables_columns"),
            "views_columns": ("databases", "views", "views_columns"),
        }
        data_source_path = "databases"


class AzureSQLGenerator(Generator):
    source = "azure"
    paths_model = AzureSQLPathsModel
    server_model = HostnameModel

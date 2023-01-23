from typing import Any, NamedTuple
from dataclasses import dataclass


@dataclass
class TableMetadata:
    table_catalog: Any
    table_schema: Any
    table_name: Any
    table_type: Any
    create_date: Any
    modify_date: Any
    type_desc: Any
    row_count: Any
    view_definition: Any
    referenced_entity_name: Any


class ColumnMetadata(NamedTuple):
    table_catalog: Any
    table_schema: Any
    table_name: Any
    column_name: Any
    ordinal_position: Any
    column_default: Any
    is_nullable: Any
    data_type: Any
    character_maximum_length: Any
    character_octet_length: Any
    numeric_precision: Any
    numeric_scale: Any
    datetime_precision: Any
    character_set_name: Any
    collation_name: Any
    column_key: Any

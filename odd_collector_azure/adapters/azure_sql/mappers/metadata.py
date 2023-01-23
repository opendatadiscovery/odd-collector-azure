from typing import Optional


_METADATA_SCHEMA_URL_PREFIX: str = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/"
    "extensions/mysql.json#/definitions/Mysql"
)

_data_set_metadata_schema_url: str = _METADATA_SCHEMA_URL_PREFIX + "DataSetExtension"
_data_set_field_metadata_schema_url: str = (
    _METADATA_SCHEMA_URL_PREFIX + "DataSetFieldExtension"
)


def convert_bytes_to_str(value: Optional[bytes]) -> Optional[str]:
    return value if type(value) is not bytes else value.decode("utf-8")

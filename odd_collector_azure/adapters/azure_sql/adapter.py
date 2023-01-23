import logging
from funcy import concat, lpluck_attr

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntityList
from oddrn_generator import MysqlGenerator

from .mappers.tables import map_table
from .repository import AzureSQLRepository
from .mappers.views import map_view
from .mappers.database import map_database


class Adapter(AbstractAdapter):
    def __init__(self, config) -> None:
        self.__config = config
        self.__azure_sql_repository = AzureSQLRepository(config)
        self.__oddrn_generator = MysqlGenerator(
            host_settings=f"{self.__config.server}.database.windows.net:{self.__config.port}",
            databases=self.__config.database
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        try:
            views_entities = [
                map_view(self.__oddrn_generator, view) for view in self.__azure_sql_repository.get_views()
            ]

            tables_entities = [
                map_table(self.__oddrn_generator, table) for table in self.__azure_sql_repository.get_tables()
            ]

            list_of_oddrns = lpluck_attr("oddrn", concat(tables_entities, views_entities))
            database_entity = map_database(self.__oddrn_generator, self.__config.database, list_of_oddrns)

            return DataEntityList(
                data_source_oddrn=self.get_data_source_oddrn(),
                items=tables_entities + views_entities + [database_entity],
            )
        except Exception as err:
            logging.error(f"Failed to load metadata for tables: {err}")
            logging.exception(Exception)

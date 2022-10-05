from oddrn_generator.generators import OdbcGenerator, MssqlGenerator, PostgresqlGenerator, Generator
from typing import Dict, Any, Type, List
from odd_models.models import DataEntity, DataEntityType
from abc import abstractmethod


class DatasourceEngine:
    def __init__(self, datasource_node: Dict[str, Any]):
        self.connection_details: Dict[str, str] = datasource_node.get('connectionDetails')

    datasource_type: str
    generator: Type[Generator]

    @abstractmethod
    def get_database_oddrn(self) -> str:
        pass

    @abstractmethod
    def map_database(self) -> DataEntity:
        pass


class JdbcDatasourceEngine(DatasourceEngine):

    def get_database_oddrn(self) -> str:
        gen = self.generator(
            host_settings=f"{self.connection_details['server']}",
            databases=self.connection_details["database"],
        )
        return gen.get_data_source_oddrn()

    def map_database(self) -> DataEntity:
        return DataEntity(
            name=self.connection_details["database"],
            oddrn=self.get_database_oddrn(),
            type=DataEntityType.DATABASE_SERVICE,
        )


class OdbcDatasourceEngine(DatasourceEngine):
    datasource_type = 'ODBC'
    generator = OdbcGenerator

    def get_database_oddrn(self) -> str:
        gen = self.generator(
            host_settings=f"{self.connection_details['connectionString']}",
            databases='database',
        )
        return gen.get_data_source_oddrn()

    def map_database(self) -> DataEntity:
        return DataEntity(
            name='database',
            oddrn=self.get_database_oddrn(),
            type=DataEntityType.DATABASE_SERVICE,
        )


class MssqlEngine(JdbcDatasourceEngine):
    datasource_type = "Sql"
    generator = MssqlGenerator


class PostgresSqlEngine(JdbcDatasourceEngine):
    datasource_type = "PostgreSql"
    generator = PostgresqlGenerator


datasources: List[Type[DatasourceEngine]] = [MssqlEngine, PostgresSqlEngine, OdbcDatasourceEngine]
datasources_factory: Dict[str, Type[DatasourceEngine]] = {
    datasource.datasource_type: datasource for datasource in datasources
}

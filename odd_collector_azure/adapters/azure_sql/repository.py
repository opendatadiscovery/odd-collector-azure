import textwrap
from typing import Dict, Iterable, List

import pytz
import pyodbc

from .domain import Dependency, DependencyType
from .domain import Table, View


TABLES_METADATA_QUERY = """
    select
        t.table_catalog,
        t.table_schema,
        t.table_name,
        t.table_type,
        s.create_date,
        s.modify_date,
        s.type_desc,
        ps.row_count
    from information_schema.tables t
        left join sys.objects s
            on
                t.TABLE_NAME = s.NAME
        left join sys.dm_db_partition_stats ps
            on
                s.object_id = ps.object_id and
                s.type_desc = 'USER_TABLE'
    where t.table_catalog = '{database}' and t.table_type = 'BASE TABLE'
"""

COLUMNS_METADATA_QUERY = """
    select
        c.table_catalog,
        c.table_schema,
        c.table_name,
        c.column_name,
        c.ordinal_position,
        c.column_default,
        c.is_nullable,
        c.data_type,
        c.character_maximum_length,
        c.character_octet_length,
        c.numeric_precision,
        c.numeric_scale,
        c.datetime_precision,
        c.character_set_name,
        c.collation_name,
        t.column_name as column_key
    from information_schema.columns c
        left join INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE t
            on
                c.table_name = t.table_name and
                c.column_name = t.column_name
    where c.table_schema <> 'sys' and c.table_name = '{table_name}'
"""

VIEW_METADATA_QUERY = """
    select
        table_catalog,
        table_schema,
        table_name,
        VIEW_DEFINITION
    from information_schema.views
    where table_schema <> 'sys'
"""

VIEW_DEPENDENCIES_QUERY = """
    select
        v.name,
        dep.referenced_entity_name,
        o.type as referenced_type
    from sys.sql_expression_dependencies dep
        left join sys.views v
            on dep.referencing_id = v.object_id
        left join sys.objects o
            on dep.referenced_id = o.object_id
"""


class AzureSQLRepository:
    def __init__(self, config):
        self.config = config
        self.connection_string = textwrap.dedent(
            '''
            Driver={driver};
            Server={server};
            Database={database};
            Uid={username};
            Pwd={password};
            Encrypt=yes;
            TrustServerCertificate=no;
            Connection Timeout=30;
            '''.format(
                driver='{ODBC Driver 18 for SQL Server}',
                database=self.config.database,
                server='{server_name}.database.windows.net,{port}'.format(server_name=self.config.server, port=self.config.port),
                username=self.config.username,
                password=self.config.password
            )
        )
        self._connection: pyodbc.Connection = pyodbc.connect(self.connection_string)
        self._cursor: pyodbc.Cursor = self._connection.cursor()

    def get_tables(self) -> Iterable[Table]:
        """Get table metadata."""
        query = TABLES_METADATA_QUERY.format(database=self.config.database)
        result = self._fetch_all(query)

        for row in result:
            table = Table(
                name=row['table_name'],
                columns=self.get_columns(row.get('table_name')),
                create_date=row['create_date'].replace(tzinfo=pytz.utc).isoformat(),
                modify_date=row['modify_date'].replace(tzinfo=pytz.utc).isoformat(),
                row_count=row['row_count'],
                description=row['type_desc'],
            )
            yield table

    def get_columns(self, table_name: str) -> List[Dict]:
        """Get columns metadata."""
        query = COLUMNS_METADATA_QUERY.format(table_name=table_name)
        columns = self._fetch_all(query)
        return columns

    def get_views(self) -> Iterable[View]:
        """Get views metadata."""
        views_data = self._fetch_all(VIEW_METADATA_QUERY)
        dependencies = self.get_view_dependencies()

        views: Dict[str, View] = {
            view.get('table_name'): View(
                name=view.get('table_name'),
                columns=self.get_columns(view.get('table_name')),
                view_definition=view.get('VIEW_DEFINITION'),
                description='',
                upstream=[],
                downstream=[],
            )
            for view in views_data
        }

        for dep in dependencies:
            if dep.name in views:
                views[dep.name].upstream.append(dep)

                if (
                    dep.referenced_type == DependencyType.VIEW
                    and dep.referenced_name in views
                ):
                    views[dep.referenced_name].downstream.append(dep)
        return iter(views.values())

    def get_view_dependencies(self) -> Iterable[Dependency]:
        """Get view dependencies."""
        dependencies = self._fetch_all(VIEW_DEPENDENCIES_QUERY)
        for row in dependencies:
            yield Dependency(
                name=row.get('name'),
                referenced_owner='',
                referenced_name=row.get('referenced_entity_name'),
                referenced_type=DependencyType(row.get('referenced_type').strip())
            )

    def _fetch_all(self, query: str) -> List[dict]:
        """Fetch data from DB and convert it to the dictionary as {column_name: value}."""
        self._cursor.execute(query)
        columns = [column[0] for column in self._cursor.description]
        results = []
        for row in self._cursor.fetchall():
            results.append(dict(zip(columns, row)))
        return results

    def __del__(self):
        self._connection.close()

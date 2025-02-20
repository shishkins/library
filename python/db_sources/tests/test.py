import pytest
from pandas import DataFrame

import config
from config import DDL
from db_sources.db import DBAPI
from db_sources.exceptions import EmptyDataError


@pytest.fixture(
    params=[
        ("tuple", tuple, config.TEST_VALUES),
        ("namedtuple", tuple, config.TEST_VALUES),
        (
            "dict",
            dict,
            [dict(zip(config.COLUMN_NAMES, row)) for row in config.TEST_VALUES],
        ),
    ]
)
def rows_type(request):
    return request.param


class Database:
    db: DBAPI = None
    ddl: DDL = None
    values: list[tuple] = config.TEST_VALUES
    df: DataFrame = DataFrame(values, columns=config.COLUMN_NAMES)

    def test_connection(self):
        self.db.get_connection()

    def test_execute_create(self):
        self.db.execute(self.ddl.create_test_table)
        self.db.execute(self.ddl.select_test_table)

    def test_insert(self):
        self.db.execute(self.ddl.truncate_test_table)
        self.db.insert(
            table=config.TABLE,
            values=self.values,
        )

    def test_execute_to_list(self, rows_type):
        data = self.db.execute_to_list(
            self.ddl.select_test_table,
            rows_type=rows_type[0],
        )

        assert isinstance(data[0], rows_type[1])

        assert data == rows_type[2]

    def test_execute_to_list_empty(self, rows_type):
        with pytest.raises(EmptyDataError):
            data = self.db.execute_to_list(
                self.ddl.select_empty_test_table,
                rows_type=rows_type[0],
                check_empty=True,
            )

    def test_execute_to_df(self):
        df = self.db.execute_to_df(self.ddl.select_test_table)

        assert isinstance(df, DataFrame)

        assert list(df.itertuples(index=False, name=None)) == self.values

    def test_execute_to_df_empty(self, rows_type):
        with pytest.raises(EmptyDataError):
            data = self.db.execute_to_df(
                self.ddl.select_empty_test_table,
                rows_type=rows_type[0],
                check_empty=True,
            )

    def test_insert_df(self):
        self.db.insert_df(df=self.df, table=config.TABLE)

    def test_execute_drop(self):
        self.db.execute(self.ddl.drop_test_table)


class TestMSSQL(Database):
    def setup_method(self):
        self.db = config.dbs.MSSQL
        self.ddl = config.mssql_ddl


class TestPostgreSQL(Database):
    def setup_method(self):
        self.db = config.dbs.PostgreSQL
        self.ddl = config.pg_ddl


class TestClickHouse(Database):
    def setup_method(self):
        self.db = config.dbs.ClickHouse
        self.ddl = config.ch_ddl

    def test_external_tables(self):
        df = self.db.execute_to_df(
            query=self.ddl.select_test_table,
            external_tables=[(self.df, "test")],
        )

        assert isinstance(df, DataFrame)

        assert list(df.itertuples(index=False, name=None)) == self.values

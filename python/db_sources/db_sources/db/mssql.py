from collections import namedtuple
from datetime import datetime
from typing import Any
from typing import Literal
from typing import Optional

import pymssql
from pandas import DataFrame
from pymssql import Connection as MSSQLConnection

from db_sources.exceptions import EmptyDataError
from ._dbapi import DBAPI
from ._util import _convert_bytes


class MSSQL(DBAPI):
    def __init__(
        self,
        host: str,
        port: int = 1433,
        database: str = "",
        user: str = "",
        password: str = "",
        provide_query: bool = False,
        provide_time: bool = False,
        nolock: bool = False,
    ):
        """
        Класс для работы с БД MSSQL

        :param host: Адрес сервера (домен/ip)
        :param port: Порт сервера
        :param database: Наименование базы данных
        :param user: Имя пользователя
        :param password: Пароль
        :param provide_query: Вывод SQL-запроса
        :param provide_time: Вывод времени выполнения SQL-запроса
        :param nolock: Отключение блокировки таблиц при SELECT запросах
        """

        super().__init__(
            host,
            port,
            database,
            user,
            password,
            provide_query,
            provide_time,
        )

        self.nolock = nolock

    @staticmethod
    def _decode_errors(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except pymssql.Error as e:
                if not isinstance(e.args[-1], str):
                    e.args = (
                        (e.args[-1][-1].decode(),)
                        if isinstance(e.args[-1], tuple)
                        else (e.args[-1].decode(),)
                    )
                raise e

        return wrapper

    @_decode_errors
    def get_connection(self, as_dict: bool = False) -> MSSQLConnection:
        """
        Получение объекта соединения с БД
        """

        connection = pymssql.connect(
            server=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            as_dict=as_dict,
        )

        return connection

    @_decode_errors
    def execute(
        self,
        query: str,
        params: Optional[dict | tuple | list] = None,
        provide_query: bool = False,
        provide_time: bool = False,
    ) -> None:
        """
        Выполнение запроса к БД

        :param query: SQL-запрос
        :param params: Параметры запроса
        :param provide_query: Вывод SQL-запроса
        :param provide_time: Вывод времени выполнения SQL-запроса
        """

        self._provide_query_info(
            query=query,
            params=params,
            provide_query=provide_query,
        )

        if self.nolock:
            query = "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;\n\n" + query

        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                start_time = datetime.now()
                cursor.execute(query, params)
                connection.commit()

        if provide_time:
            print(f"| {'elapsed_time':>12} : {datetime.now() - start_time}")

    @_decode_errors
    def execute_to_list(
        self,
        query: str,
        params: Optional[dict | tuple | list] = None,
        with_columns: bool = False,
        rows_type: Literal["tuple", "dict", "namedtuple"] = "tuple",
        convert_bytes: bool | Literal["uuid", "str"] = False,
        provide_query: bool = False,
        provide_time: bool = False,
        check_empty: bool = False,
    ) -> Optional[list[tuple] | list[dict] | tuple[list[tuple], Any]]:
        """
        Выполнение запроса к БД и возвращение ответа в виде списка строк

        :param query: SQL-запрос
        :param params: Параметры запроса
        :param with_columns: Получение столбцов и их типов
        :param rows_type: Тип данных (строк) в полученном списке
        :param convert_bytes: Конвертация bytes значений в uuid.
            При True или "uuid" - возвращает тип UUID, при str - возвращает строку
        :param provide_query: Вывод SQL-запроса
        :param provide_time: Вывод времени выполнения SQL-запроса
        :param check_empty: Вызов ошибки при отсутствии данных в результате запроса
        """

        self._provide_query_info(
            query=query,
            params=params,
            provide_query=provide_query,
        )

        if self.nolock:
            query = "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;\n\n" + query

        with self.get_connection(as_dict=True if rows_type == "dict" else False) as connection:
            with connection.cursor() as cursor:
                start_time = datetime.now()
                cursor.execute(query, params)
                rows = cursor.fetchall()
                columns = [column[0] for column in cursor.description]
                connection.commit()

            if provide_time:
                print(f"| {'elapsed_time':>12} : {datetime.now() - start_time}")

            if convert_bytes:
                rows = _convert_bytes(rows, as_uuid=False if convert_bytes == "str" else True)

            if rows_type == "namedtuple":
                nt_columns = []
                for column in columns:
                    column = str(column)
                    column = "f" + column if column.startswith("_") else column
                    nt_columns.append(column)
                Row_ = namedtuple("Row", nt_columns)
                rows = [Row_(*row) for row in rows]

            if check_empty and not rows:
                raise EmptyDataError('Запрос вернул пустой результат!')

            if with_columns:
                return rows, columns

            return rows

    def execute_to_df(
        self,
        query: str,
        params: Optional[dict | tuple | list] = None,
        convert_bytes: bool | Literal["uuid", "str"] = False,
        provide_query: bool = False,
        provide_time: bool = False,
        check_empty: bool = False,
        **kwargs,
    ) -> DataFrame:
        """
        Выполнение SQL-запроса к БД и возвращение результата в виде DataFrame

        :param query: SQL-запрос
        :param params: Параметры запроса
        :param convert_bytes: Конвертация bytes значений в uuid.
            При True или "uuid" - возвращает тип UUID, при str - возвращает строку
        :param provide_query: Вывод SQL-запроса
        :param provide_time: Вывод времени выполнения SQL-запроса
        :param check_empty: Вызов ошибки при отсутствии данных в результате запроса

        :return DataFrame
        """

        rows, columns = self.execute_to_list(
            query=query,
            params=params,
            with_columns=True,
            convert_bytes=convert_bytes,
            provide_query=provide_query or self.provide_query,
            provide_time=provide_time or self.provide_time,
            check_empty=check_empty,
            **kwargs,
        )
        df = DataFrame(rows, columns=columns)
        return df

    @_decode_errors
    def insert(
        self,
        table: str,
        values: list[tuple] | list[list],
        columns: Optional[list] = None,
        schema: Optional[str] = None,
        truncate: bool = False,
    ) -> None:
        """
        Вставка данных в БД

        :param table: Наименование таблицы. Поддерживается формат: schema.table, table
        :param values: Значения
        :param columns: Наименования колонок
        :param schema: Наименование схемы / БД
        :param truncate: Очистить таблицу перед вставкой
        """
        schema_table = f"{schema}.{table}" if schema else table

        if not values:
            print("it's nothing to insert")
            return

        if columns:
            columns = ", ".join([f'"{str(column)}"' for column in columns])
            columns = f"({columns})"

        placeholders = ", ".join(["%s" for _ in values[0]])
        query = f"INSERT INTO {schema_table} {columns if columns else ''} VALUES ({placeholders})"

        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                if truncate:
                    cursor.execute(f"TRUNCATE TABLE {schema_table};")
                cursor.executemany(query, values)

            connection.commit()

    def insert_df(
        self,
        df: DataFrame,
        table: str,
        schema: Optional[str] = None,
        truncate: bool = False,
        create_table: bool = False,
    ) -> None:
        """
        Вставка DataFrame в БД

        :param df: DataFrame
        :param table: Наименование таблицы. Поддерживается формат: schema.table, table
        :param schema: Наименование схемы / БД
        :param truncate: Очистить таблицу перед вставкой
        :param create_table: Создание таблицы при вставке
        """

        if create_table:
            self.execute(self.generate_ddl(df=df, table=table, schema=schema))

        self.insert(
            values=df.to_numpy(na_value=None, dtype=object).tolist(),
            columns=df.columns.tolist(),
            table=table,
            schema=schema,
            truncate=truncate,
        )

    def generate_ddl(
        self,
        df: DataFrame,
        table: str,
        schema: str = None,
    ) -> str:
        """
        Генерация DDL таблицы на основе DataFrame

        :param df: DataFrame
        :param table: Наименование таблицы. Поддерживается формат: schema.table, table
        :param schema: Наименование схемы / БД
        """

        schema_table = f"{schema}.{table}" if schema else table

        dtype_mapping = {
            "int": "int",
            "float": "float",
            "Decimal": "numeric(15, 2)",
            "str": "nvarchar",
            "UUID": "uniqueidentifier",
            "date": "date",
            "Timestamp": "datetime",
            "bool": "bit",
            "bytes": "binary(16)",
        }

        columns = []

        for value, column in zip(
            df.iloc[0].to_numpy(na_value=None, dtype=object).tolist(), df.columns
        ):
            col_type = "nvarchar"

            for dtype in dtype_mapping:
                if dtype in str(type(value)):
                    col_type = dtype_mapping[dtype]
                    break

            columns.append(f'"{column}" {col_type}')

        columns_ddl = ",\n".join(columns)
        ddl = f"CREATE TABLE IF NOT EXISTS {schema_table} (\n{columns_ddl}\n)\n"

        return ddl

    def truncate(
        self,
        table: str,
        schema: Optional[str] = None,
    ) -> None:
        """
        Очистка таблицы

        :param table: Наименование таблицы. Поддерживается формат: schema.table, table
        :param schema: Наименование схемы / БД
        """
        schema_table = f"{schema}.{table}" if schema else table
        self.execute(f"TRUNCATE TABLE {schema_table}")

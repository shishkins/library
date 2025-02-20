import time
import warnings
from collections import namedtuple
from datetime import datetime
from typing import Any
from typing import Literal
from typing import Optional

from clickhouse_driver import Client
from clickhouse_driver.dbapi.connection import Connection
from pandas import DataFrame

from db_sources.exceptions import EmptyDataError, PartitionsNotFoundError
from ._dbapi import DBAPI
from ._util import click_df_to_table, _convert_bytes


class ClickHouse(DBAPI):
    def __init__(
        self,
        host: str,
        port: int = 9000,
        database: str = "",
        user: str = "default",
        password: str = "",
        provide_query: bool = False,
        provide_time: bool = False,
        connect_timeout: int = 10,
        send_receive_timeout: int = 300,
        settings: Optional[dict] = None,
        sync: bool = False,
        wait_after_insert: int = None,
    ) -> None:
        """
        Класс для работы с БД Clickhouse

        :param host: Адрес сервера (домен/ip)
        :param port: Порт сервера
        :param database: Наименование базы данных
        :param user: Имя пользователя
        :param password: Пароль
        :param provide_query: Вывод SQL-запроса
        :param provide_time: Вывод времени выполнения SQL-запроса
        :param connect_timeout: Максимальное время соединения с сервером
        :param send_receive_timeout: Максимальное время ожидания ответа от сервера
        :param settings: Словарь с параметрами (https://clickhouse.com/docs/en/operations/settings/settings)
        :param sync: Синхронное ожидание выполнения запросов на всех репликах
        :param wait_after_insert: Ожидание после выполнения insert/insert_df (сек)
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

        self.connect_timeout = connect_timeout
        self.send_receive_timeout = send_receive_timeout
        self.settings = settings
        self.wait_after_insert = wait_after_insert

        if sync:
            self.settings = self.settings or dict()
            self.settings["alter_sync"] = 2
            self.settings["mutations_sync"] = 2
            self.settings["wait_for_async_insert"] = 1

    def get_client(self) -> Client:
        """
        Метод для получения клиента для подключения к БД ClickHouse

        :return: клиент для подключения к БД ClickHouse
        """

        return Client(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            connect_timeout=self.connect_timeout,
            send_receive_timeout=self.send_receive_timeout,
            settings=self.settings,
        )

    def get_connection(self) -> Connection:
        """
        Получение объекта соединения с БД

        :return: подключения к БД ClickHouse
        """

        return self.get_client().connection

    def execute(
        self,
        query: str,
        params: Optional[dict | tuple | list] = None,
        provide_query: bool = False,
        provide_time: bool = False,
        external_tables: Optional[list[tuple[DataFrame, str]] | list[dict]] = None,
        settings: Optional[dict] = None,
        query_id: Optional[str] = None,
        **kwargs,
    ) -> None:
        """
        Выполнение запроса к БД

        :param query: SQL-запрос
        :param params: Параметры запроса
        :param provide_query: Вывод SQL-запроса
        :param provide_time: Вывод времени выполнения SQL-запроса
        :param external_tables: Внешние таблицы
        :param settings: Словарь с параметрами
        :param query_id: Идентификатор SQL-запроса
        """

        self._provide_query_info(
            query=query,
            params=params,
            provide_query=provide_query,
            settings=settings,
        )
        if external_tables:
            if isinstance(external_tables[0][0], DataFrame):
                external_tables = [
                    click_df_to_table(table[0], table[1]) for table in external_tables
                ]

        with self.get_client() as client:
            start_time = datetime.now()
            client.execute(
                query=query,
                params=params,
                external_tables=external_tables,
                settings=settings,
                query_id=query_id,
                **kwargs,
            )

            if provide_time:
                print(f"| {'elapsed_time':>12} : {datetime.now() - start_time}")

    def execute_to_list(
        self,
        query: str,
        params: Optional[dict | tuple | list] = None,
        with_columns: bool = False,
        rows_type: Literal["tuple", "dict", "namedtuple"] = "tuple",
        convert_bytes: bool | Literal["uuid", "str"] = False,
        provide_query: bool = False,
        provide_time: bool = False,
        external_tables: Optional[list[tuple[DataFrame, str]] | list[dict]] = None,
        settings: Optional[dict] = None,
        query_id: Optional[str] = None,
        check_empty: bool = False,
        **kwargs,
    ) -> Optional[list[tuple] | list[dict] | tuple[list[tuple], Any]]:
        """
        Выполнение SQL-запроса к БД и возвращение ответа в виде списка строк

        :param query: SQL-запрос
        :param params: Параметры запроса
        :param with_columns: Получение столбцов и их типов
        :param rows_type: Тип данных (строк) в полученном списке
        :param convert_bytes: Конвертация bytes значений в uuid.
            При True или "uuid" - возвращает тип UUID, при str - возвращает строку
        :param provide_query: Вывод SQL-запроса
        :param provide_time: Вывод времени выполнения SQL-запроса
        :param external_tables: Внешние таблицы
        :param settings: Словарь с параметрами
        :param query_id: Идентификатор SQL-запроса
        :param check_empty: Вызов ошибки при отсутствии данных в результате запроса
        """

        self._provide_query_info(
            query=query,
            params=params,
            provide_query=provide_query,
            settings=settings,
        )

        if external_tables:
            if isinstance(external_tables[0][0], DataFrame):
                external_tables = [
                    click_df_to_table(table[0], table[1]) for table in external_tables
                ]

        with self.get_client() as client:
            start_time = datetime.now()
            rows, columns = client.execute(
                query=query,
                params=params,
                with_column_types=True,
                external_tables=external_tables,
                settings=settings,
                query_id=query_id,
                **kwargs,
            )

            if provide_time:
                print(f"| {'elapsed_time':>12} : {datetime.now() - start_time}")

            if convert_bytes:
                rows = _convert_bytes(rows, as_uuid=False if convert_bytes == "str" else True)

            columns = [column[0] for column in columns]

            if rows_type == "dict":
                rows = [dict(zip(columns, row)) for row in rows]
            elif rows_type == "namedtuple":
                nt_columns = []
                for column in columns:
                    column = str(column)
                    column = "f" + column if column.startswith("_") else column
                    nt_columns.append(column)
                Row_ = namedtuple("Row", nt_columns)
                rows = [Row_(*row) for row in rows]

            if check_empty and not rows:
                raise EmptyDataError("Запрос вернул пустой результат!")

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
        external_tables: Optional[list[tuple[DataFrame, str]] | list[dict]] = None,
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
        :param external_tables: Внешние таблицы
        :param check_empty: Вызов ошибки при отсутствии данных в результате запроса

        :return DataFrame
        """

        rows, columns = self.execute_to_list(
            query=query,
            params=params,
            with_columns=True,
            convert_bytes=convert_bytes,
            provide_query=provide_query,
            provide_time=provide_time,
            external_tables=external_tables,
            check_empty=check_empty,
            **kwargs,
        )
        df = DataFrame(rows, columns=columns)
        return df

    def insert(
        self,
        table: str,
        values: list[tuple] | list[list],
        columns: Optional[list] = None,
        schema: Optional[str] = None,
        truncate: bool = False,
        wait_after_insert: int = None,
    ) -> None:
        """
        Вставка данных в БД

        :param table: Наименование таблицы. Поддерживается формат: schema.table, table
        :param values: Значения
        :param columns: Наименования колонок
        :param schema: Наименование схемы / БД
        :param truncate: Очистить таблицу перед вставкой
        :param wait_after_insert: Ожидание после выполнения запроса (сек)
        """
        schema_table = f"{schema}.{table}" if schema else table

        if not values:
            print("it's nothing to insert")
            return

        if columns:
            columns = ", ".join([f'"{str(column)}"' for column in columns])
            columns = f"({columns})"

        if truncate:
            self.execute(f"TRUNCATE TABLE {schema_table};")

        self.execute(
            f"insert into {schema_table} {columns if columns else ''} values",
            values,
            settings={"input_format_null_as_default": True},
        )

        if wait_after_insert is None:
            wait_after_insert = self.wait_after_insert

        if wait_after_insert:
            time.sleep(wait_after_insert)

    def insert_df(
        self,
        df: DataFrame,
        table: str,
        schema: Optional[str] = None,
        truncate: bool = False,
        create_table: bool = False,
        order_by: list = None,
        wait_after_insert: int = None,
    ) -> None:
        """
        Вставка DataFrame в БД

        :param df: DataFrame
        :param table: Наименование таблицы. Поддерживается формат: schema.table, table
        :param schema: Наименование схемы / БД
        :param truncate: Очистить таблицу перед вставкой
        :param create_table: Создание таблицы при вставке
        :param order_by: Список столбцов для ключа сортировки. Используется при create_table=True
        :param wait_after_insert: Ожидание после выполнения запроса (сек)
        """

        if order_by and not create_table:
            warnings.warn("Параметр order_by будет проигнорирован при create_table = False!")

        if create_table:
            if not order_by:
                raise ValueError("Параметр order_by обязателен при create_table = True!")

            ddl = self.generate_ddl(
                df=df,
                table=table,
                schema=schema,
                order_by=order_by,
            )
            self.execute(ddl)
            time.sleep(0.5)

        self.insert(
            values=df.to_numpy(na_value=None, dtype=object).tolist(),
            columns=df.columns.tolist(),
            table=table,
            schema=schema,
            truncate=truncate,
            wait_after_insert=wait_after_insert,
        )

    def generate_ddl(
        self,
        df: DataFrame,
        table: str,
        schema: str = None,
        order_by: list = None,
    ) -> str:
        """
        Генерация DDL таблицы на основе DataFrame

        :param df: DataFrame
        :param table: Наименование таблицы. Поддерживается формат: schema.table, table
        :param schema: Наименование схемы / БД
        :param order_by: Список столбцов для ключа сортировки
        """

        schema_table = f"{schema}.{table}" if schema else table

        if not order_by:
            raise ValueError("Параметр order_by обязателен!")

        dtype_mapping = {
            "int": "Int32",
            "float": "Float64",
            "Decimal": "Decimal(15, 2)",
            "str": "String",
            "UUID": "UUID",
            "date": "Date",
            "Timestamp": "DateTime",
            "bool": "Bool",
        }

        columns = []

        for value, column in zip(
            df.iloc[0].to_numpy(na_value=None, dtype=object).tolist(),
            df.columns,
        ):
            col_type = "String"

            for dtype in dtype_mapping:
                if dtype in str(type(value)):
                    col_type = dtype_mapping[dtype]
                    break

            columns.append(f'"{column}" {col_type}')

        columns_ddl = ",\n".join(columns)
        order_by_ddl = ", ".join([f'"{column}"' for column in order_by])
        ddl = (
            f"CREATE TABLE IF NOT EXISTS {schema_table} (\n{columns_ddl}\n)\n"
            f"ENGINE = MergeTree\nORDER BY ({order_by_ddl})"
        )

        return ddl

    def truncate(
        self,
        table: str,
        schema: Optional[str] = None,
        cluster: str = None,
    ) -> None:
        """
        Очистка таблицы

        :param table: Наименование таблицы. Поддерживается формат: schema.table, table
        :param schema: Наименование схемы / БД
        :param cluster: Наименование кластера, на котором необходимо выполнить операцию
        """
        schema_table = f"{schema}.{table}" if schema else table
        query = f"TRUNCATE TABLE {schema_table}"

        if cluster:
            query += f" ON CLUSTER {cluster}"

        self.execute(query)

    def exchange(
        self,
        first_table: str,
        second_table: str,
        first_schema: str = None,
        second_schema: str = None,
        cluster: str = None,
    ) -> None:
        """
        Меняет наименования 2 таблиц атомарно

        :param first_table: Наименование первой таблицы. Поддерживается формат: schema.table, table
        :param second_table: Наименование второй таблицы. Поддерживается формат: schema.table, table
        :param first_schema: Наименование схемы / БД первой таблицы
        :param second_schema: Наименование схемы / БД второй таблицы
        :param cluster: Наименование кластера, на котором необходимо выполнить операцию
        """

        first_schema_table = f"{first_schema}.{first_table}" if first_schema else first_table
        second_schema_table = f"{second_schema}.{second_table}" if second_schema else second_table
        query = f"EXCHANGE TABLES {first_schema_table} AND {second_schema_table}"

        if cluster:
            query += f" ON CLUSTER {cluster}"

        self.execute(query)

    def copy_with_partition(
        self,
        source_table: str,
        target_table: str,
        source_schema: str = None,
        target_schema: str = None,
        truncate: bool = True,
        cluster: str = None,
    ) -> None:
        """
        Копирование всех данных из одной таблицы в другую с помощью партиций

        :param source_table: Наименование таблицы источника. Поддерживается формат: schema.table, table
        :param target_table: Наименование таблицы получателя. Поддерживается формат: schema.table, table
        :param source_schema: Наименование схемы / БД таблицы источника
        :param target_schema: Наименование схемы / БД таблицы получателя
        :param truncate: Очистка таблицы получателя перед копированием партиций
        :param cluster: Наименование кластера, на котором необходимо выполнить операцию
        """
        source_schema_table = f"{source_schema}.{source_table}" if source_schema else source_table
        target_schema_table = f"{target_schema}.{target_table}" if target_schema else target_table
        cluster_query = f"ON CLUSTER {cluster}" if cluster else ""

        partitions = self.execute_to_list(
            f"SELECT DISTINCT partition "
            f"FROM system.parts "
            f"WHERE database || '.' || table = '{source_schema_table}' "
            f"AND partition_id != 'all'"
        )

        if not partitions:
            raise PartitionsNotFoundError("В исходной таблице нет партиций!")

        if truncate:
            self.truncate(schema=target_schema, table=target_table, cluster=cluster)

        for partition in partitions:
            self.execute(
                f"ALTER TABLE {target_schema_table} {cluster_query} "
                f"ATTACH PARTITION {partition} FROM {source_schema_table}"
            )

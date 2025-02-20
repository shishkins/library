from datetime import datetime
from typing import Any, Literal
from typing import Optional
from typing import Type

import psycopg
from pandas import DataFrame
from psycopg import Cursor
from psycopg.rows import Row, RowFactory, dict_row, namedtuple_row

from db_sources.exceptions import EmptyDataError
from ._dbapi import DBAPI
from ._util import _convert_bytes


class PostgreSQL(DBAPI):
    def __init__(
        self,
        host: str,
        port: int = 5432,
        database: str = "postgres",
        user: str = "postgres",
        password: str = "",
        provide_query: bool = False,
        provide_time: bool = False,
        cursor_factory: Optional[Type[Cursor[Row]]] = None,
        row_factory: Optional[RowFactory[Any]] = None,
    ) -> None:
        """
        Класс для работы с БД MSSQL

        :param host: Адрес сервера (домен/ip)
        :param port: Порт сервера
        :param database: Наименование базы данных
        :param user: Имя пользователя
        :param password: Пароль
        :param provide_query: Вывод SQL-запроса
        :param provide_time: Вывод времени выполнения SQL-запроса
        :param cursor_factory: Фабрика курсоров
        :param row_factory: Фабрика строк
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

        self.cursor_factory = cursor_factory
        self.row_factory = row_factory

    def get_connection(self, **kwargs) -> psycopg.connection.Connection:
        """
        Получение объекта соединения с БД
        """

        return psycopg.connect(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=self.password,
            cursor_factory=self.cursor_factory,
            row_factory=self.row_factory,
            **kwargs,
        )

    def execute(
        self,
        query: str,
        params: Optional[dict | tuple | list] = None,
        provide_query: bool = False,
        provide_time: bool = False,
        **kwargs,
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

        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                start_time = datetime.now()
                cursor.execute(query, params, **kwargs)
                connection.commit()

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
        check_empty: bool = False,
        **kwargs,
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

        if rows_type == "dict":
            row_factory = dict_row
        elif rows_type == "namedtuple":
            row_factory = namedtuple_row
        else:
            row_factory = None

        with self.get_connection() as connection:
            with connection.cursor(row_factory=row_factory) as cursor:
                start_time = datetime.now()
                cursor.execute(query, params, **kwargs)

                while cursor.nextset():
                    pass

                rows = cursor.fetchall()
                columns = [column[0] for column in cursor.description]
                connection.commit()

            if provide_time:
                print(f"| {'elapsed_time':>12} : {datetime.now() - start_time}")

            if convert_bytes:
                rows = _convert_bytes(
                    rows,
                    rows_type=rows_type,
                    as_uuid=False if convert_bytes == "str" else True,
                )
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

    def insert(
        self,
        table: str,
        values: list[tuple] | list[list],
        columns: Optional[list] = None,
        schema: Optional[str] = None,
        truncate: bool = False,
        method: Literal["copy", "execute"] = "copy",
    ) -> None:
        """
        Вставка данных в БД

        :param table: Наименование таблицы. Поддерживается формат: schema.table, table
        :param values: Значения
        :param columns: Наименования колонок
        :param schema: Наименование схемы / БД
        :param truncate: Очистить таблицу перед вставкой
        :param method: Метод вставки данных
        """
        schema_table = f"{schema}.{table}" if schema else table

        if not values:
            print("it's nothing to insert")
            return

        if columns:
            columns = ", ".join([f'"{str(column)}"' for column in columns])
            columns = f"({columns})"

        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                if truncate:
                    cursor.execute(f"TRUNCATE TABLE {schema_table};")

                if method == "copy":
                    with cursor.copy(
                        f"COPY {schema_table} {columns if columns else ''} FROM STDIN"
                    ) as copy:
                        for row in values:
                            copy.write_row(row)
                else:
                    placeholders = ", ".join(["%s" for _ in values[0]])
                    query = f"INSERT INTO {schema_table} {columns if columns else ''} VALUES ({placeholders})"
                    cursor.executemany(query, values)

            connection.commit()

    def insert_df(
        self,
        df: DataFrame,
        table: str,
        schema: Optional[str] = None,
        truncate: bool = False,
        create_table: bool = False,
        method: Literal["copy", "execute"] = "copy",
    ) -> None:
        """
        Вставка DataFrame в БД

        :param df: DataFrame
        :param table: Наименование таблицы. Поддерживается формат: schema.table, table
        :param schema: Наименование схемы / БД
        :param truncate: Очистить таблицу перед вставкой
        :param create_table: Создание таблицы при вставке
        :param method: Метод вставки данных
        """

        if create_table:
            self.execute(self.generate_ddl(df=df, table=table, schema=schema))

        self.insert(
            values=df.to_numpy(na_value=None, dtype=object).tolist(),
            columns=df.columns.tolist(),
            table=table,
            schema=schema,
            truncate=truncate,
            method=method,
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
            "int": "integer",
            "float": "real",
            "Decimal": "numeric(15, 2)",
            "str": "varchar",
            "UUID": "uuid",
            "date": "date",
            "Timestamp": "timestamp",
            "bool": "boolean",
            "bytes": "bytea",
        }

        columns = []

        for value, column in zip(
            df.iloc[0].to_numpy(na_value=None, dtype=object).tolist(), df.columns
        ):
            col_type = "varchar"

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

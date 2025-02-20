from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import Literal
from typing import Optional

from pandas import DataFrame

from ._util import substitute_params


class DBAPI(ABC):
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        provide_query: bool = False,
        provide_time: bool = False,
    ) -> None:
        """
        Абстрактный класс подключения к базе данных

        :param host: Адрес сервера (домен/ip)
        :param port: Порт сервера
        :param database: Наименование базы данных
        :param user: Имя пользователя
        :param password: Пароль
        :param provide_query: Вывод SQL-запроса
        :param provide_time: Вывод времени выполнения SQL-запроса
        """

        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

        self.provide_query = provide_query
        self.provide_time = provide_time

    def __repr__(self) -> str:
        return self.host

    def _provide_query_info(
        self,
        query: str,
        params: Optional[dict | tuple | list],
        settings: Optional[dict] = None,
        provide_query: bool = False,
    ) -> None:
        provide_query = provide_query or self.provide_query
        with_params = isinstance(params, dict | tuple | list) and len(params) > 0
        with_settings = (
            self.__class__.__name__ == "ClickHouse"
            and isinstance(settings, dict)
            and (len(settings) > 0)
        )

        if not provide_query:
            return

        print("----")

        raw_query = " ".join(query.split())
        print(f"| {'raw_query':>12} : {raw_query}")

        if with_params:
            if isinstance(params, dict):
                query_ = substitute_params(query=raw_query, params=params)
                print(f"| {'query':>12} : {query_}")

            params_str = f"{len(params)=}"
            if len(params) < 20:
                params_str += f" : {params}"
            print(f"| {'params':>12} : {params_str}")

        if with_settings:
            settings_str = f"{len(settings)=}"
            if len(settings) < 20:
                settings_str += f" : {settings}"
            print(f"| {'settings':>12} : {settings_str}")

        return

    @abstractmethod
    def get_connection(self) -> Any:
        """
        Получение объекта подключения к БД
        """
        ...

    @abstractmethod
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
        ...

    @abstractmethod
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
        ...

    @abstractmethod
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
        ...

    @abstractmethod
    def insert(
        self,
        table: str,
        values: list[tuple],
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
        ...

    @abstractmethod
    def insert_df(
        self,
        df: DataFrame,
        table: str,
        schema: Optional[str] = None,
        truncate: bool = False,
    ) -> None:
        """
        Вставка DataFrame в БД

        :param df: DataFrame
        :param table: Наименование таблицы. Поддерживается формат: schema.table, table
        :param schema: Наименование схемы / БД
        :param truncate: Очистить таблицу перед вставкой
        """
        ...

    @abstractmethod
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
        ...

    @abstractmethod
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
        ...

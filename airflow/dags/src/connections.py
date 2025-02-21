import os

from airflow.models import Variable
from db_sources import PostgreSQL, ClickHouse, MSSQL

from src.prometheus import get_least_loaded_node


def pg_conn(database: str = 'postgres', **kwargs) -> PostgreSQL:
    login_key = "PG_LOGIN"
    password_key = "PG_PASSWORD"

    connection = PostgreSQL(
        host="pg-host.ru",
        database=database,
        user=os.getenv(login_key) or Variable.get(login_key),
        password=os.getenv(password_key) or Variable.get(password_key),
        **kwargs,
    )
    return connection


def ch_conn(node: int | str = 3, **kwargs) -> ClickHouse:
    login_key = "CH_LOGIN"
    password_key = "CH_PASSWORD"
    if node == "balance":
        node = get_least_loaded_node()
    connection = ClickHouse(
        host=f"ch-{node}.host.ru",
        database="default",
        user=os.getenv(login_key) or Variable.get(login_key),
        password=os.getenv(password_key) or Variable.get(password_key),
        **kwargs,
    )
    return connection


def mssqlconn(database: str = "dns_com", **kwargs) -> MSSQL:
    login_key = "MSSQL_LOGIN"
    password_key = "MSSQL_PASSWORD"

    connection = MSSQL(
        host="mssql-host.ru",
        database=database,
        user=os.getenv(login_key) or Variable.get(login_key),
        password=os.getenv(password_key) or Variable.get(password_key),
        **kwargs,
    )
    return connection


def pg_airflow(localhost: bool = True, **kwargs) -> PostgreSQL:
    """
    Фукнция соединения с бэкендом AirFlow
    :param localhost: флаг localhost, необходим, если функция вызывается из airflow
    :type localhost: bool
    :return: соединение с PostgreSQL
    :rtype: класс db_sources
    """
    login_key = "PG_AIRFLOW_LOGIN"
    password_key = "PG_AIRFLOW_PASSWORD"
    host_key = "PG_AIRFLOW_HOST"

    connection = PostgreSQL(
        host="airflow-db" if localhost else os.getenv(host_key) or Variable.get(host_key),
        database="airflow",
        user=os.getenv(login_key) or Variable.get(login_key),
        password=os.getenv(password_key) or Variable.get(password_key),
        **kwargs,
    )
    return connection

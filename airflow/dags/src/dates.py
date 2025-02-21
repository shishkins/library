import enum
import inspect
from datetime import datetime, date, timedelta, time
from typing import Generator, Callable
from zoneinfo import ZoneInfo

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.models.dag import get_last_dagrun
from airflow.utils.db import provide_session
from croniter import croniter
from dateutil.relativedelta import relativedelta
from db_sources import MSSQL, PostgreSQL, ClickHouse
from pandas import date_range, Timestamp


class DateFrequency(enum.Enum):
    """
    Перечисление для описания стандартных периодичностей получения дат.
    """

    day = "D"
    week = "W-MON"
    month = "MS"


def get_dates_between(
    start_date: date | str,
    end_date: date | str = None,
    frequency: DateFrequency = None,
    as_set=False,
) -> list[date] | set[date]:
    """
    Получение дат по заданной периодичности между двумя датами.

    :param start_date: дата начала периода.
    :param end_date: дата окончания периода.
    :param frequency: периодичность получаемых дат.
    :param as_set: возвращать ли массив дат в виде множества.
    :return: даты между двумя датами.
    """
    if end_date is None:
        end_date = datetime.now(ZoneInfo("Asia/Vladivostok")).date()
    if frequency is None:
        frequency = DateFrequency.day

    dates: Generator = (
        timestamp.date()
        for timestamp in date_range(start=start_date, end=end_date, freq=frequency.value)
    )

    if as_set:
        return set(dates)
    else:
        return list(dates)


def date_generator(
    frequency: relativedelta,
    start_date: str,
    end_date: str,
    with_start_date: bool = True,
    with_end_date: bool = False,
) -> Generator[str, None, None]:
    """
    Генерирует даты (str) в формате '%Y-%m-%d %H:%M:%S' или '%Y-%m-%d' с заданной периодичностью
    от начала до конца периода включительно.

    Пример использования:

    from dateutil.relativedelta import relativedelta

    generator = date_generator(
        start_date='2022-02-01',
        end_date='2022-03-01',
        frequency=relativedelta(weeks=1)
    )

    next(generator)
    '2022-02-01'
    next(generator)
    '2022-02-08'
    next(generator)
    '2022-02-15'
    next(generator)
    '2022-02-22'
    next(generator)
    '2022-03-01'


    :param frequency: Периодичность.
    :param start_date: Начало периода.
    :param end_date: Конец периода.
    :return: Генератор дат.
    """
    try:
        start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
        end_date = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    if not with_start_date:
        start_date += frequency

    assert start_date < end_date, f"{start_date=} bigger or equal {end_date=}"

    while start_date < end_date:
        yield str(start_date)
        start_date += frequency
    else:
        if start_date == end_date:
            yield str(end_date)
        else:
            if with_end_date:
                yield str(end_date)


def get_existing_dates(
    connection: MSSQL | PostgreSQL | ClickHouse,
    schema_table: str,
    column: str,
    column_to_date: bool = False,
) -> set[date]:
    if column_to_date:
        query = f"select distinct toDate({column}) as {column} from {schema_table};"
    else:
        query = f"select distinct {column} from {schema_table};"

    existing_dates_df = connection.execute_to_df(query)
    existing_dates: set[date] = {
        i.date() if type(i) == Timestamp else i for i in existing_dates_df[column]
    }
    return existing_dates


def get_missing_dates(
    connection: MSSQL | PostgreSQL | ClickHouse,
    schema_table: str,
    column: str,
    start_date: date | str,
    end_date: date | str = None,
    frequency: DateFrequency = None,
    as_set: bool = False,
    column_to_date: bool = False,
) -> list[date] | set[date]:
    """
    Получение отсутствующих в таблице дат.

    :param connection: подключение к БД.
    :param schema_table: схема и таблица.
    :param column: поле с датой в таблице.
    :param start_date: дата начала периода.
    :param end_date: дата окончания периода.
    :param frequency: периодичность получаемых дат.
    :param as_set: возвращать ли массив дат в виде множества.
    :param column_to_date: Приведение datetime/timestamp в таблице к date
    :return: отсутствующие в таблице даты.
    """
    if frequency is None:
        frequency = DateFrequency.day

    target_dates = get_dates_between(start_date, end_date, frequency, as_set=True)

    existing_dates = get_existing_dates(connection, schema_table, column, column_to_date)

    missing_dates = target_dates - existing_dates

    if as_set:
        return missing_dates
    else:
        return sorted(missing_dates)


def add_date(target_date: datetime.date, count_days: int) -> date:
    """
    Добавление дней к дате.

    :param target_date: изначальная дата.
    :param count_days: количество дней для сложения.
    :return: дата с добавленными днями.
    """
    return target_date + timedelta(days=count_days)


def get_date_vl_now() -> date:
    """
    Получение текущей даты по Владивостоку.

    :return: какая дата сейчас во Владивостоке.
    """
    return datetime.now(ZoneInfo("Asia/Vladivostok")).date()


def get_datetime_vl_now() -> datetime:
    """
    Получение текущей даты и времени по Владивостоку.

    :return: какая дата и время сейчас во Владивостоке.
    """
    return datetime.now(ZoneInfo("Asia/Vladivostok"))


def get_start_of_month_vl(as_string: bool = False) -> datetime | str:
    """
    Получение текущей даты и времени начала месяца по Владивостоку.

    :return: какая дата начала месяца сейчас во Владивостоке.
    """
    today = datetime.now(ZoneInfo("Asia/Vladivostok"))

    if as_string:
        return str(datetime(today.year, today.month, 1))
    return datetime.combine(today.date().replace(day=1), time(), today.tzinfo)


def get_start_of_week_vl(as_string: bool = True) -> datetime | str:
    """
    Получение текущей даты и времени начала месяца по Владивостоку.

    :return: какая дата начала месяца сейчас во Владивостоке.
    """
    today = datetime.now(ZoneInfo("Asia/Vladivostok"))
    start_of_week = datetime(today.year, today.month, today.day) - relativedelta(
        days=today.weekday()
    )

    return str(start_of_week) if as_string else start_of_week


def with_schedule_interval(task: Callable) -> Callable:
    """
    Декоратор для присваивания Airflow task (python_callable)
    собственного (отличного от DAG) schedule_interval.

    :param task: Task, который будет передан на вход PythonOperator параметру python_callable.
    :return: Декоратор
    """

    def wrapper(
        *args, schedule_interval: str, base_datetime: datetime = None, **kwargs
    ) -> Callable | AirflowSkipException:
        """
        Wrapper.

        :param schedule_interval: Крон строка
        :param base_datetime: Дата, относительно которой считается запуск task.
        По умолчанию: относительно запуска DAG.
        :return: AirflowSkipException в случае, если время не совпадает. Иначе task.
        """
        if not base_datetime:
            start_datetime_dag: pendulum.DateTime = kwargs["next_execution_date"]
            base_datetime = datetime(
                start_datetime_dag.year,
                start_datetime_dag.month,
                start_datetime_dag.day,
                start_datetime_dag.hour,
                start_datetime_dag.minute,
                0,
            ) + timedelta(hours=10)

        task_kwargs = {key: kwargs[key] for key in inspect.getfullargspec(task).args}
        if croniter.match(schedule_interval, base_datetime):
            return task(*args, **task_kwargs)

        raise AirflowSkipException

    return wrapper


def last_execution_date_getter(dag_id: str) -> Callable:
    """
    По идентификатору DAG возвращает функцию, которая возвращает последнюю дату запуска DAG.

    :param dag_id: Идентификатор DAG
    :return: Функция последней даты запуска.
    """

    @provide_session
    def _get_execution_date_of_dag(exec_date, session=None, **kwargs):
        dag_last_run = get_last_dagrun(dag_id, session)
        print("last_execution_date:", dag_last_run.execution_date)
        return dag_last_run.execution_date

    return _get_execution_date_of_dag

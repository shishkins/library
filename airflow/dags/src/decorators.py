from functools import wraps

from airflow.operators.python import get_current_context
from airflow.operators.python import AirflowSkipException
from src.airflow_defaults.utils import AirflowHosts
from airflow.exceptions import AirflowException


def get_testing_params(suffix: str = ""):
    """
    Декоратор, отвечающий за получение параметров для перехода на тестовый запуск задачи.
    Имеется зависимость от хоста Airflow.
    :param suffix: пользовательское наименование суффикса для тестовых витрин
    """

    def decorator(task):
        @wraps(task)
        def wrapper(*args, **kwargs):
            try:
                context = get_current_context()
                base_url = context["conf"].get("webserver", "BASE_URL")
                table_suffix = ""
                if "dev" in base_url:
                    table_suffix = suffix if suffix else "_test"
            except AirflowException:
                table_suffix = suffix if suffix else "_test"
            return task(*args, **kwargs, table_suffix=table_suffix)

        return wrapper

    return decorator


def skip_airflow_task(task):
    """
    Декоратор вызывает срабатывание AirflowSkipException, если не требуется, чтобы таск выполнялся на DEV Airflow
    """

    @wraps(task)
    def wrapper(*args, **kwargs):
        context = get_current_context()
        base_url = context["conf"].get("webserver", "BASE_URL")

        if "dev" in base_url:
            raise AirflowSkipException

        return task(*args, **kwargs)

    return wrapper

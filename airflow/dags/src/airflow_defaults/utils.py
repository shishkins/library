import os
import re
from datetime import timedelta, datetime
from pathlib import Path
from traceback import format_exception_only
from typing import Callable
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.models import DagRun, SlaMiss, TaskInstance
from airflow.models.dag import ScheduleIntervalArg
from airflow.utils.email import send_email_smtp
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from airflow_defaults.users import Position
from airflow_defaults.users import get_emails, get_names
from telegram import send_message


class AirflowDAG(DAG):
    def __init__(
            self,
            dag_id: str,
            description: str = None,
            schedule_interval: ScheduleIntervalArg = None,
            owner: str | list[str] = None,
            email: list[str] = None,
            dag_file: str = None,
            tags: list[str] = None,
            catchup: bool = False,
            max_active_runs: int = 1,
            start_date=datetime(2023, 1, 1),
            telegram_on_failure: bool = False,
            retries: int = 2,
            retry_delay: ScheduleIntervalArg = timedelta(minutes=5),
            sla: timedelta = timedelta(hours=1),
            **kwargs,
    ):
        doc_md = None
        if email is None:
            email = get_emails()
        if owner is None:
            default_owner = get_names(Position.department_head)[0]
            owner = f'by default {default_owner}'

        args = {
            "owner": owner,
            "email": email,
            "depends_on_past": False,
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": retries,
            "retry_delay": retry_delay,
            "telegram_on_failure": telegram_on_failure
        }
        default_args = kwargs.pop("default_args", None)
        args.update({"on_failure_callback": self.failure_callback})

        if default_args:
            args.update(default_args)

        # if sla:
        #     args.update({"sla": sla})

        if dag_file:
            try:
                path_parts = Path(dag_file).parts
                path_parts = path_parts[: path_parts.index("dags") + 3]
                readme_path = Path().joinpath(*path_parts, "README.md")

                with open(readme_path, "r") as f:
                    doc_md = f.read()
            except FileNotFoundError:
                raise FileNotFoundError("Не найден README.md в корне папки DAG!")

        super().__init__(
            dag_id=dag_id,
            description=description,
            schedule_interval=schedule_interval,
            tags=tags,
            catchup=catchup,
            max_active_runs=max_active_runs,
            start_date=start_date,
            default_args=args,
            doc_md=doc_md,
            # sla_miss_callback=self._sla_miss_callback if sla else None,
            **kwargs,
        )

    @staticmethod
    def _sla_miss_callback(
            dag: DAG,
            task_list: str,
            blocking_task_list: str,
            slas: list[SlaMiss],
            blocking_tis: list[TaskInstance],
    ):
        sla = str(dag.default_args["sla"])
        dag_id = dag.dag_id
        dag_url = f"{os.getenv('AIRFLOW__WEBSERVER__BASE_URL')}/dags/{dag.dag_id}"

        message = (
            f"<b>⚠️ AIRFLOW - Превышено время выполнения! ⚠️</b>\n\n"
            f"<b>Макс. время выполнения:</b> {sla}\n"
            f"<b>DAG:</b> {dag_id}\n"
            f"<b>Tasks:</b>\n"
            f"<pre>{task_list}</pre>"
        )

        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton(text="Открыть DAG", url=dag_url))
        send_message(-4279132110, message, markup)

    @staticmethod
    def failure_callback(context: dict):
        telegram_on_failure = context.get("dag").default_args['telegram_on_failure']
        tags = context.get("dag").tags
        AirflowDAG._email_alert(context)
        if telegram_on_failure or "critical" in tags:
            AirflowDAG._telegram_alert(context)

    @staticmethod
    def _telegram_alert(context: dict, chats: list = None):
        chats = [chat_id]  # chat_ID
        dag_id = context.get("dag").dag_id
        task_id = context.get("task").task_id
        owner_id = context.get("dag").owner
        description = context.get("dag").description

        start_date = context.get("ti").start_date
        start_date = start_date.astimezone(ZoneInfo("Asia/Vladivostok")).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        end_date = context.get("ti").end_date
        end_date = end_date.astimezone(ZoneInfo("Asia/Vladivostok")).strftime("%Y-%m-%d %H:%M:%S")

        exception = format_exception_only(context.get("exception"))
        exception = "".join(exception).strip()
        exception = re.sub(r"[<>]", " ", exception)

        message = (
            f"<b>❌ AIRFLOW - Ошибка выполнения! ❌</b>\n"
            f"<b>DAG:</b> {dag_id}\n"
            f"<b>Task:</b> {task_id}\n"
            f"<b>Дата старта:</b> {start_date}\n"
            f"<b>Дата падения:</b> {end_date}\n\n"
            f"<b>Владелец:</b> {owner_id}\n"
            f"<b>Описание:</b> {description}\n"
            f"<b>Ошибка:</b>\n\n"
            f'<pre language="python">{exception}</pre>'
        )

        markup = InlineKeyboardMarkup()
        log_url = InlineKeyboardButton(text="Перейти к логу", url=context.get("ti").log_url)
        success_url = InlineKeyboardButton(
            text="Пометить успешным", url=context.get("ti").mark_success_url
        )
        markup.add(log_url, success_url)

        for chat_id in chats:
            send_message(chat_id, message, markup)

    @staticmethod
    def _email_alert(context: dict):
        dag_id = context.get("dag").dag_id
        task_id = context.get("task").task_id
        owner_id = context.get("dag").owner
        description = context.get("dag").description
        email = context.get("dag").default_args['email']
        logs_link = context.get("ti").log_url
        mark_success = context.get("ti").mark_success_url
        title = f"Airflow FSPB alert: {dag_id} failed on {task_id}"

        start_date = context.get("ti").start_date
        start_date = start_date.astimezone(ZoneInfo("Asia/Vladivostok")).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        end_date = context.get("ti").end_date
        end_date = end_date.astimezone(ZoneInfo("Asia/Vladivostok")).strftime("%Y-%m-%d %H:%M:%S")

        exception = format_exception_only(context.get("exception"))
        exception = "".join(exception).strip()
        exception = re.sub(r"[<>]", " ", exception)

        message = (
            f"<b>❌ AIRFLOW - Ошибка выполнения! ❌</b><br><br>"
            f"<b>DAG:</b> {dag_id}<br>"
            f"<b>Task:</b> {task_id}<br>"
            f"<b>Дата старта:</b> {start_date}<br>"
            f"<b>Дата падения:</b> {end_date}<br><br>"
            f"<b>Владелец:</b> {owner_id}<br>"
            f"<b>Описание:</b> {description}<br>"
            f"<b>Ошибка:</b><br>"
            f"{exception}\<br><br>"
            f'Ссылка на логи: <a href="{logs_link}">Logs link</a>'
        )

        send_email_smtp(email, title, message)  # отправка email

def get_last_execution_date(dag_id: str) -> Callable:
    """
    Получить дату последнего запуска DAG по расписанию

    Args:
        dag_id: Наименование DAG
    """

    def get_external_execution_date(execution_date, **kwargs):
        dag_runs = DagRun.find(dag_id=dag_id)
        dag_runs.sort(key=lambda x: x.start_date, reverse=True)
        if dag_runs:
            return dag_runs[0].execution_date

    return get_external_execution_date

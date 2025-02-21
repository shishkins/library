import json
from enum import Enum
from uuid import UUID
import os
from airflow.models.variable import Variable
from datetime import datetime
from pytz import timezone
from dateutil.relativedelta import relativedelta
import uuid

import pandas as pd
from confluent_kafka import Consumer, Message, Producer


class KafkaBrokers(Enum):
    """Брокеры Kafka"""

    def __repr__(self) -> str:
        return ",".join(self._value_)

    def __str__(self) -> str:
        return ",".join(self._value_)

    @property
    def list(self) -> list:
        return self.value

    test = [
        "dev-host-kafka-1.ru:9092",
        "dev-host-kafka-2:9092",
        "dev-host-kafka-3:9092",
    ]


# depricated
class UUIDEncoder(json.JSONEncoder):
    """
    Класс энкодинга UUID в json / костыль с stackoverflow
    """

    def default(self, obj):
        if isinstance(obj, UUID):
            # if the obj is uuid, we simply return the value of uuid
            return obj.hex
        return json.JSONEncoder.default(self, obj)


def create_json_list(df, json_formatter):
    """
    функция для форматирования списка json-ов
    :param df: таблица DataFrame для форматирование в json-list
    :param json_formatter: [форматирование таблицы в json-list]
    :return: list: [dict, dict, dict ...]
    """
    json_list = []

    for _, row in df.iterrows():
        json_dict = dict()
        json_dict["topic"] = json_formatter["topic"]
        if json_formatter.get("key"):
            json_dict["key"] = ";".join([str(row[key]) for key in json_formatter["key"]])
        else:
            json_dict["key"] = str(uuid.uuid1())
        json_dict["value"] = dict(
            row[json_formatter["value"].keys()].rename(json_formatter["value"])
        )
        if json_formatter.get('headers'):
            json_dict["headers"] = dict(row[json_formatter["headers"].keys()].rename(json_formatter["headers"]))
        json_list.append(json_dict)

    return json_list


def get_upd_del_df(
    df_current: pd.DataFrame,
    df_new: pd.DataFrame,
    columns_delete: list,
    columns_update: list,
    column_types: dict = None,
) -> pd.DataFrame:
    """
    Получение DataFrame для обновления и удаления записей в Kafka
    на основе текущего и нового состояния данных

    Args:
        df_current: Текущее состояние данных
        df_new: Новое состояние данных
        columns_delete: колонки, по которым будет происходить сверка (merge) на удаление
        columns_update: колонки, по которым будет происходить сверка (merge) на добавление/обновление
        column_types: типы колонок

    Returns:
        DataFrame с ключами соединения и operation = del или upd
    """

    merge_del_df = pd.merge(
        df_current, df_new[columns_delete], on=columns_delete, how="outer", indicator=True
    )
    del_data_df = (
        merge_del_df[merge_del_df["_merge"] == "left_only"].drop(["_merge"], axis=1).copy()
    )
    del_data_df["operation"] = "del"
    print(f"Count delete rows: {del_data_df.shape[0]}")

    upd_data_df = pd.merge(
        df_current[columns_update], df_new, on=columns_update, how="outer", indicator=True
    )
    upd_data_df = upd_data_df[upd_data_df["_merge"] == "right_only"].drop(["_merge"], axis=1).copy()
    upd_data_df["operation"] = "upd"
    print(f"Count update rows: {upd_data_df.shape[0]}")

    send_data_kafka = pd.concat([del_data_df, upd_data_df], ignore_index=True, sort=False)

    if column_types:
        send_data_kafka = send_data_kafka.astype(column_types)

    return send_data_kafka


def get_all_data_from_topic(
    topic: str,
    bootstrap_servers: KafkaBrokers | str,
    name_group: str,
    chunk_size=10000,
) -> (list[dict], Consumer):
    """
    Получение всех данных из kafka, что находятся в указанном топике.
    Если было обращение ранее, где был совершен consumer.commit() для указанной группы,
    то получение всех данных будет с этой засечки.

    Args:
        topic: целевой топик.
        bootstrap_servers: кластер кафки, гле находится топик.
        name_group: наименование группы под которой будет обращение к топику.
        chunk_size: размер сообщения для получения.

    Returns:
        list[json]
    """
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": name_group,
            "auto.offset.reset": "beginning",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])

    data = []
    chunk_size = chunk_size

    while True:
        messages: list[Message] = consumer.consume(timeout=10, num_messages=chunk_size)
        messages = [json.loads(msg.value()) for msg in messages]
        data += messages
        if not messages:
            consumer.close()
            break
    return data, consumer


def delivery_callback(err, msg):
    """
    Функция для обработки метаданных об отправке сообщений
    :param err: ошибка
    :param msg:
    :return:
    """
    if err is not None:
        print(
            f"Ошибка доставки: {err}\nСообщение: {msg.value()}, \n topic: {msg.topic()}, \n partition: {msg.partition()}, \n offset: {msg.offset()}"
        )


def send_data_to_kafka(json_list: list[dict], bootstrap_server) -> None:
    prd = Producer(
        {
            "bootstrap.servers": bootstrap_server,
            "queue.buffering.max.messages": 10000000,
            "enable.idempotence": True,  # включить идемпотентную доставку,
        }
    )
    for message in json_list:
        headers = message.get("headers")
        part_key = str(message["key"])
        topic = message["topic"]
        try:
            prd.poll(0)
            prd.produce(
                topic=topic,
                value=json.dumps(message["value"], ensure_ascii=False, default=str),
                key=part_key,
                headers=headers,
                callback=delivery_callback,
            )
        except BufferError:
            print("Local producer queue is full, trying again")

            prd.poll(0)
            prd.produce(
                topic=topic,
                value=json.dumps(message["value"], ensure_ascii=False, default=str),
                key=part_key,
                headers=headers,
                callback=delivery_callback,
            )

    prd.flush()
    if prd.__len__() == 0:
        print("Сообщения успешно доставлены")


def get_latest_message_from_topic(
    topic: str,
    bootstrap_servers: KafkaBrokers | str,
    name_group: str = None,
):
    if name_group is None:
        name_group = os.getenv("KAFKA_CONSUMER_GROUP") or Variable.get("KAFKA_CONSUMER_GROUP")
    settings = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": name_group,
        "enable.auto.commit": False,
        "session.timeout.ms": 6000,
        "default.topic.config": {"auto.offset.reset": "largest"},
    }
    consumer = Consumer(settings)

    def on_assign(a_consumer, partitions):
        last_offset = a_consumer.get_watermark_offsets(partitions[0])
        partitions[0].offset = last_offset[1] - 1
        consumer.assign(partitions)

    consumer.subscribe([topic], on_assign=on_assign)

    message = consumer.poll(6.0)
    if message is None:
        return datetime.now(timezone("Asia/Vladivostok")) - relativedelta(days=2)
    _date = datetime.fromtimestamp(message.timestamp()[1] / 1000)
    consumer.close()
    return _date


# Шаблон для json_formatter-a совпадает с форматом json-а для отправки в кафку
# json_formatter = {
#     "topic": None,
#     "key": None,
#     "value": {  #тело сообщения
#         "column_1": None,
#         "column_2": None,
#         "column_3": None
#     },
#     "headers": {  #необходимые тэги, берутся из колонок df
#         '_operation_': None
#     }
# }

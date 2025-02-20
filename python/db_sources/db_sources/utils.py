import os
from base64 import b64encode
from binascii import hexlify, unhexlify
from typing import Literal
from uuid import UUID

import requests
from pandas._libs import NaTType
from pandas._libs.missing import NAType


def convert_binary_to_guid(
        binary: bytes | str,
        as_uuid: bool = False,
) -> str | UUID | None:
    """
    Функция конвертирования Binary ID в GUID

    :param binary: Binary ID (bytes или hex строка)
    :param as_uuid: Тип возвращаемого GUID: строка или UUID
    :return: GUID
    """

    if isinstance(binary, float | NAType | NaTType | None):
        return

    if isinstance(binary, str):
        binary = unhexlify(binary.replace("0x", ""))

    if binary == bytes(0):
        guid = UUID(int=0)
        return guid if as_uuid else str(guid)

    binary = hexlify(binary).decode('ascii').zfill(32)
    guid = "-".join(
        [
            binary[24:],
            binary[20:24],
            binary[16:20],
            binary[0:4],
            binary[4:16],
        ]
    )
    return UUID(guid) if as_uuid else guid


def convert_guid_to_binary(
        guid: str | UUID,
        length: Literal[16, 8, 4] = 16,
        as_hex: bool = False,
) -> bytes | str | None:
    """
    Функция конвертирования GUID в Binary ID

    :param guid: GUID
    :param length: Длина GUID. По умолчанию - 16
    :param as_hex: Формат возвращаемого Binary ID: bytes или hex строка
    :return: Binary ID
    """

    if isinstance(guid, NAType | NaTType | None):
        return

    if isinstance(guid, UUID):
        guid = guid.hex
    else:
        guid = guid.replace("-", "")

    binary = "".join(
        [
            guid[17 - 1: 17 - 1 + 4],
            guid[21 - 1: 21 - 1 + 12],
            guid[13 - 1: 13 - 1 + 4],
            guid[9 - 1: 9 - 1 + 4],
            guid[1 - 1: 1 - 1 + 8],
        ]
    ).upper()

    binary = binary[-length * 2:]

    if as_hex:
        return "0x" + binary

    return unhexlify(binary)


def get_variables(
        login: str = None,
        password: str = None,
        host: str = None
):
    """
    Функция получения переменных окружения из Airflow

    :param login: Логин Airflow. Если не указан, берётся переменная <AIRFLOW_LOGIN>
    :param password: Пароль Airflow. Если не указан, берётся переменная <AIRFLOW_PASSWORD>
    :param host: Адрес Airflow

    """
    if login is None:
        login = os.getenv("AIRFLOW_LOGIN")

    if password is None:
        password = os.getenv("AIRFLOW_PASSWORD")

    if not (login is None and password is None):
        api_key = f"{login}:{password}".encode("ASCII")
        api_key = f"Basic {b64encode(api_key).decode()}"
    else:
        raise AttributeError("Не указан логин и/или пароль!")

    response = requests.get(
        url=f"{host}/api/v1/variables",
        headers={"Authorization": api_key},
        params={"limit": 1000},
    )

    if response.status_code == 200:
        variables: list[dict] = response.json()["variables"]
    else:
        response.raise_for_status()
        return

    for variable in variables:
        key = variable.get("key")
        value = variable.get("value")
        os.environ[key] = value

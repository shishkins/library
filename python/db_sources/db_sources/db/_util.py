from binascii import hexlify
from datetime import date
from datetime import datetime
from datetime import time
from enum import Enum
from typing import Any, Literal
from uuid import UUID

from pandas import DataFrame


def _refactor_param(param):
    match param:
        case None:
            return "NULL"
        case datetime():
            return f"'{param.strftime('%Y-%m-%dT%H:%M:%S')}'"
        case date():
            return f"'{param.strftime('%Y-%m-%dT00:00:00')}'"
        case time():
            return f"'{param.strftime('%H:%M:%S')}'"
        case str():
            return f"'{param.encode('unicode_escape').decode()}'"
        case list():
            return f"[{', '.join(str(_refactor_param(x)) for x in param)}]"
        case tuple():
            return f"({', '.join(str(_refactor_param(x)) for x in param)})"
        case Enum():
            return _refactor_param(param.value)
        case UUID():
            return f"'{str(param)}'"
    return param


def substitute_params(
    query: str,
    params: dict,
) -> str:
    if not params:
        return query

    if not isinstance(params, dict):
        raise ValueError("Parameters are expected in dict form")

    params_ = {key: _refactor_param(value) for key, value in params.items()}

    return query % params_


def click_gen_struct(df: DataFrame) -> list[tuple[Any, str]]:
    """
    Функция для генерирования структуры таблицы из DataFrame

    :param df: DataFrame
    :return: структура таблицы
    """

    structure = []

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

    for value, column in zip(
        df.iloc[0].to_numpy(na_value=None, dtype=object).tolist(),
        df.columns,
    ):
        col_type = "String"

        for dtype in dtype_mapping:
            if dtype in str(type(value)):
                col_type = dtype_mapping[dtype]
                break

        structure.append((column, col_type))

    return structure


def click_df_to_table(df: DataFrame, table_name: str = None) -> dict:
    """
    Функция для создания таблицы для ClickHouse из DataFrame

    :param df: DataFrame
    :param table_name: имя таблицы в ClickHouse
    :return: словарь вида: {'name': ..., 'structure': ..., 'data': ...}
    """

    table_structure: list[tuple[Any, str]] = click_gen_struct(df)

    # Для приведения столбцов object к string
    df.loc[:, (df.dtypes == object)] = df.loc[:, (df.dtypes == object)].map(str)

    table = {
        "name": table_name,
        "structure": table_structure,
        "data": df.to_numpy(na_value=None, dtype=object).tolist(),
    }
    return table


def _convert_bytes(
    rows: list = None,
    rows_type: Literal["tuple", "dict", "namedtuple"] = "tuple",
    as_uuid: bool = True,
) -> Any:
    """
    Функция конвертации bytes в uuid

    :param rows: результат Select запроса
    :param as_uuid: Тип возвращаемого uuid: str или UUID
    :return: Список кортежей или словарей
    """

    def convert_value(value: Any):
        if isinstance(value, bytes) and len(value) <= 16:
            if value == bytes(0):
                guid = UUID(int=0)
                return guid if as_uuid else str(guid)

            hex_string = hexlify(value).decode("ascii").zfill(32)
            guid = "-".join(
                [
                    hex_string[24:],
                    hex_string[20:24],
                    hex_string[16:20],
                    hex_string[0:4],
                    hex_string[4:16],
                ]
            )
            return UUID(guid) if as_uuid else guid
        return value

    if rows_type == "dict":
        return [
            {key: convert_value(value) for key, value in row.items()} for row in rows
        ]
    elif rows_type == "namedtuple":
        raise NotImplementedError(
            'Конвертация bytes в uuid при rows_type = "namedtuple" не поддерживается.'
        )
    else:
        return [tuple(convert_value(value) for value in row) for row in rows]

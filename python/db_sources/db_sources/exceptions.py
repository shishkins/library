
class EmptyDataError(Exception):
    """Ошибка при отсутствии данных в результате запроса"""
    ...


class PartitionsNotFoundError(Exception):
    """Ошибка при отсутствии партиций у таблицы"""
    ...

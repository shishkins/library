import enum


class Position(enum.Enum):
    """
    Перечисление для описания должностей.
    """

    department_head = "Руководитель отдела"
    data_engineer = "Инженер данных"
    data_analyst = "Аналитик данных"
    fired = "Уволен"


class Staff:
    def __init__(
            self,
            name: str,
            email: str,
            position: Position,
    ):
        """
        Сотрудник отдела.

        :param name: Имя.
        :param email: email-адрес.
        :param position: Должность.
        """
        self.name: str = name
        self.email: str = email
        self.position: Position = position


tretyakov = Staff(
    name="Tretyakov.AA",
    email="Tretyakov.AA@mail.ru",
    position=Position.data_engineer,
)



def get_all_staff() -> tuple[Staff, ...]:
    """
    Получение кортежа всех сотрудников.

    :return: кортеж всех сотрудников.
    """
    return (
        tretyakov
    )


def get_emails(*positions: Position) -> list[str]:
    """
    Получение списка email-адресов.

    :param positions: Перечисление должностей. Если не указано, то будут использованы все должности.
    :return: список email-адресов.
    """
    emails: list[str] = sorted(
        staff.email
        for staff in get_all_staff()
        if ((not positions and staff.position is not None) or (staff.position in positions))
    )
    return emails


def get_names(*positions: Position) -> list[str]:
    """
    Получение списка пользователей класса Staff

    :param positions: Перечисление должностей. Если не указано, то будут использованы все должности.
    :return: список email-адресов.
    """
    names: list[str] = sorted(
        staff.name
        for staff in get_all_staff()
        if ((not positions and staff.position is not None) or (staff.position in positions))
    )
    return names

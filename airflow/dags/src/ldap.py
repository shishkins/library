import os

import ldap
from airflow.models import Variable
from ldap.controls import SimplePagedResultsControl


def get_from_ldap(host: str = None,
                  username: str = None,
                  password: str = None,
                  base: str = None,
                  search_flt: str = None,
                  search_attrlist: list = None) -> list:
    """
    Функция получения данных из LDAP по заданному фильтру и атрибутам

    Args:
        host: адрес сервера LDAP. По умолчанию: partner.ru
        username: логин LDAP. По умолчанию: AD_LOGIN
        password: пароль LDAP. По умолчанию: AD_PASSWORD
        base: базовая директория LDAP. По умолчанию: OU=DNS Users,DC=partner,DC=ru
        search_flt: фильтр поиска. Опционально.
        search_attrlist: атрибуты, которые необходимо получить. Опционально.

    Returns:
        list: Список словарей с данными из LDAP
    """
    if host is None:
        host = 'partner.ru'
    if username is None:
        username = os.getenv('AD_LOGIN') or Variable.get('AD_LOGIN')
    if password is None:
        password = os.getenv('AD_PASSWORD') or Variable.get('AD_PASSWORD')
    if base is None:
        base = 'OU=DNS Users,DC=partner,DC=ru'

    connect = ldap.initialize(f"ldap://{host}")
    connect.set_option(ldap.OPT_REFERRALS, 0)
    connect.simple_bind_s(username, password)

    page_size = 500
    req_ctrl = SimplePagedResultsControl(criticality=True, size=page_size, cookie='')
    msgid = connect.search_ext(
        base=base,
        scope=ldap.SCOPE_SUBTREE,
        filterstr=search_flt,
        attrlist=search_attrlist,
        serverctrls=[req_ctrl]
    )

    total_results = []
    pages = 0
    while True:
        pages += 1
        rtype, rdata, rmsgid, serverctrls = connect.result3(msgid)
        for user in rdata:
            total_results.append(user)

        pctrls = [c for c in serverctrls if c.controlType == SimplePagedResultsControl.controlType]
        if pctrls:
            if pctrls[0].cookie:
                req_ctrl.cookie = pctrls[0].cookie
                msgid = connect.search_ext(
                    base=base,
                    scope=ldap.SCOPE_SUBTREE,
                    filterstr=search_flt,
                    attrlist=search_attrlist,
                    serverctrls=[req_ctrl]
                )
            else:
                break
        else:
            break
    return total_results

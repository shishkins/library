import json
import time
import pandas as pd
import uuid
from datetime import datetime,timedelta

from source_orp.algorithm_template.src import queries
from source_orp.algorithm_template.src import helper

from src.connections import pg_conn, ch_conn

pg_conn = pg_conn()
ch_conn = ch_conn()
def task_1(context: dict):

    now_pg_df = pg_conn.execute_to_df(
        queries.query_pg_1
    )

    now_ch_df = ch_conn.execute_to_df(
        queries.create_1
    )

    user_pg_df = pg_conn.execute_to_df(
        queries.query_pg_2
    )

    user_ch_df = ch_conn.execute_to_df(
        queries.create_2
    )

    helper.great_function_1('a', 'r', 'g', 's', kwargs={'kwargs':'kwargs'})
    helper.great_function_2('a', 'r', 'g', 's', kwargs={'kwargs':'kwargs'})
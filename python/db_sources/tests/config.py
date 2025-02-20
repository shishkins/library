from collections import namedtuple

from db_sources import ClickHouse
from db_sources import MSSQL
from db_sources import PostgreSQL

TABLE = "test"
COLUMN_NAMES = ["id", "attr"]
TEST_VALUES = [
    (1, "attr1"),
    (2, "attr2"),
]

DBS = namedtuple(
    "DBS",
    [
        "ClickHouse",
        "PostgreSQL",
        "MSSQL",
    ],
)

dbs = DBS(
    ClickHouse(
        host="localhost",
        user="default",
    ),
    PostgreSQL(
        host="localhost",
        database="postgres",
        user="postgres",
        password="StrongPassword1",
    ),
    MSSQL(
        host="localhost",
        database="master",
        user="sa",
        password="StrongPassword1",
        provide_query=True,
    ),
)

DDL = namedtuple(
    "DDL",
    [
        "create_test_table",
        "truncate_test_table",
        "select_test_table",
        "select_empty_test_table",
        "drop_test_table",
    ],
)

ch_ddl = DDL(
    create_test_table=""" create table default.test
    (
        id Int32,
        attr String
    )
        engine = MergeTree
            order by id
            settings index_granularity = 8192 
""",
    truncate_test_table="""truncate table test""",
    select_test_table="""select * from test""",
    select_empty_test_table="""select * from test limit 0""",
    drop_test_table="""drop table test""",
)

mssql_ddl = DDL(
    create_test_table="""create table test (id INT, attr VARCHAR (50));""",
    truncate_test_table="""truncate table test""",
    select_test_table="""select * from test""",
    select_empty_test_table="""select top 0  * from test""",
    drop_test_table="""drop table test""",
)


pg_ddl = DDL(
    create_test_table=""" create table test
(
    id int4, 
    attr text
)
""",
    truncate_test_table="""truncate table test""",
    select_test_table="""select * from test""",
    select_empty_test_table="""select * from test limit 0""",
    drop_test_table="""drop table test""",
)

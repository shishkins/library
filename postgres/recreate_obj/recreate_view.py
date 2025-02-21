from db_sources.utils import get_variables
get_variables()
from datetime import datetime
from db_sources import PostgreSQL
import pandas as pd


from ddl_views_backup.src.main import DDLViewsBackup
from queries import get_depend_objects


def pg_execute_queries(queries: list[dict] | list, connect: PostgreSQL) -> None:
    db = pg_fspb()

    with db.get_connection() as connection:
        with connection.cursor() as cursor:
            for query in queries:
                print(query["text"])
                cursor.execute(query["text"], query["params"])
            connection.commit()


need_obj = {
    "schema": "data_mart", # схема объекта
    "obj": "products_internal_prices", # наименование объекта
    "object_type": "VIEW", # relkind
}

drop_target = {
    "text": """
DROP {object_type} IF EXISTS {schema}.{obj} CASCADE;
""".format(
        schema=need_obj["schema"], obj=need_obj["obj"], object_type=need_obj["object_type"]
    ),
    "params": {},
}

target_ddl = {
    "text": """

""",
    "params": {},
}


DDLView_instance = DDLViewsBackup(connection=PostgreSQL(), group="all")
dependent_objects = PostgreSQL().execute_to_df(
    get_depend_objects.format(schema=need_obj["schema"], obj=need_obj["obj"])
)
dependent_objects["need_ddl"] = None
dependent_ddl = pd.DataFrame()

for _, dependent_object in dependent_objects.iterrows():
    if dependent_object["materialized"]:
        ddl = DDLView_instance.formatting_ddl_matview(
            schema_name=dependent_object["schema"],
            matview=dependent_object["name"],
        )
        drop_ddl = f'DROP MATERIALIZED VIEW {dependent_object["schema"]}.{dependent_object["name"]};'
    else:
        ddl = DDLView_instance.formatting_ddl_view(
            schema_name=dependent_object["schema"], view=dependent_object["name"]
        )
        drop_ddl = f'DROP VIEW {dependent_object["schema"]}.{dependent_object["name"]};'
    if dependent_object["name"] == need_obj["obj"] and dependent_object["schema"] == need_obj["schema"]:
        continue
    dependent_ddl = pd.concat(
        [
            dependent_ddl,
            pd.DataFrame(
                {
                    "need_ddl": [ddl],
                    "schema": [dependent_object["schema"]],
                    "obj": [dependent_object["name"]],
                    "lvl": [dependent_object["lvl"]],
                    "drop_ddl": [drop_ddl],
                }
            ),
        ]
    )
dependent_ddl = dependent_ddl.sort_values(by=["lvl"], ascending=[True])
dependent_ddl.drop_duplicates(subset=["schema", "obj"], inplace=True)

drop_dependent_ddls = list(
    map(lambda x: {"text": x, "params": {}}, dependent_ddl["drop_ddl"].to_list()),
)

recreate_ddls = list(
    map(lambda x: {"text": x, "params": {}}, dependent_ddl["need_ddl"].to_list()),
)

pg_execute_queries(connect=PostgreSQL(), queries=[drop_target] + [target_ddl] + recreate_ddls)

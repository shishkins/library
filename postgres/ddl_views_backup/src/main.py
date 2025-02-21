import db_sources as db

from ddl_views_backup.src.queries.ddl import *
from ddl_views_backup.src.queries.pg_fspb import *


class DDLViewsBackup:

    def __init__(self, connection: db.PostgreSQL = None, group=None):
        if connection is None:
            raise AttributeError("Не задано подключение к БД")
        if group is None:
            print("Не указана роль\nВыбраны все схемы по умолчанию")
            self._schemas = self.__class__.get_list_schemas("all", connection)
        else:
            self._schemas = self.__class__.get_list_schemas(group=group, connection=connection)

        self._connection = connection
        self._dict_of_data = {
            "create": self._connection.execute_to_df(
                get_relation_query.format(schemas=repr(self._schemas))
            ),
            "comments": self._connection.execute_to_df(
                get_relation_comments.format(schemas=repr(self._schemas))
            ),
            "indexes": self._connection.execute_to_df(
                get_relation_indexes.format(schemas=repr(self._schemas))
            ),
        }

    @staticmethod
    def get_list_schemas(group: str, connection: db.PostgreSQL) -> tuple | list:
        lists_schemas_by_group = {
            "engineer": (
                "1c_theta",
                "backup",
                "cold_backup",
                "data_mart",
                "reprices_manager",
                "tech_info",
            ),
            "sudo": (
                "alg_accessories",
                "alg_actualization",
                "alg_first_track",
                "alg_kbt",
                "alg_lineup",
                "alg_margin_turnover",
                "alg_overstock",
                "alg_pc",
                "alg_seasonal",
                "alg_segment_plan",
                "alg_small_price",
                "alg_small_stock",
                "alg_third_track",
                "alg_turnover",
                "alg_turnover_normalization",
                "dashboard",
                "mdl_commercial_allowance",
                "mdl_course",
                "mdl_margin",
                "metrics_orp",
                "restrictive_prices_calculation",
                "seasonality",
            ),
            "test": ("test", "test"),
        }
        match group:
            case "all":
                res = connection.execute_to_list(query=list_schemas_all)
                return tuple([item[0] for item in res])
            case "engineer":
                return lists_schemas_by_group["engineer"]
            case "sudo":
                return lists_schemas_by_group["sudo"]
            case "test":
                return lists_schemas_by_group["test"]
            case _:
                return tuple()

    @property
    def backup_list(self):
        objects = self._connection.execute_to_df(
            get_relation_query.format(schemas=repr(self._schemas))
        )
        list_of_objects = objects[["schema_name", "rel_name"]].values.tolist()
        return list(map(lambda obj: '.'.join(obj), list_of_objects))

    def formatting_ddl_view(self, view, schema_name):
        ddl_dict = self._dict_of_data.copy()

        ddl_df = ddl_dict["create"].loc[
            (ddl_dict["create"]["rel_name"] == view)
            & (ddl_dict["create"]["schema_name"] == schema_name)
            ]
        comments_df = ddl_dict["comments"].loc[
            (ddl_dict["comments"]["rel_name"] == view)
            & (ddl_dict["comments"]["schema_name"] == schema_name)
            ]

        schema_name = ddl_df["schema_name"].iloc[0]
        view_name = ddl_df["rel_name"].iloc[0]
        view_text = ddl_df["view_text"].iloc[0]
        view_comment = ddl_df["comment"].iloc[0]

        view_file = f'\nCREATE OR REPLACE VIEW "{schema_name}"."{view_name}" AS\n{view_text};\n'

        for schema_name, view_name, attname, comment in zip(
                comments_df["schema_name"],
                comments_df["rel_name"],
                comments_df["attname"],
                comments_df["comment"],
        ):
            if comment is None:
                continue
            comment_text = f"""\nCOMMENT ON COLUMN "{schema_name}"."{view_name}"."{attname}" IS '{comment}';\n"""
            view_file += comment_text

        if view_comment is not None:
            view_file += (
                f"""\nCOMMENT ON VIEW "{schema_name}"."{view_name}" is '{view_comment}';\n"""
            )

        return view_file

    def formatting_ddl_matview(self, schema_name, matview):
        ddl_dict = self._dict_of_data.copy()

        ddl_df = ddl_dict["create"].loc[
            (ddl_dict["create"]["rel_name"] == matview)
            & (ddl_dict["create"]["schema_name"] == schema_name)
            ]
        indexes_df = ddl_dict["indexes"].loc[
            (ddl_dict["indexes"]["rel_name"] == matview)
            & (ddl_dict["indexes"]["schema_name"] == schema_name)
            ]
        comments_df = ddl_dict["comments"].loc[
            (ddl_dict["comments"]["rel_name"] == matview)
            & (ddl_dict["comments"]["schema_name"] == schema_name)
            ]

        schema_name = ddl_df["schema_name"].iloc[0]
        matview_name = ddl_df["rel_name"].iloc[0]
        matview_text = ddl_df["view_text"].iloc[0]
        matview_comment = ddl_df["comment"].iloc[0]

        matview_file = (
            f'\nCREATE MATERIALIZED VIEW "{schema_name}"."{matview_name}" AS\n{matview_text};\n'
        )

        for schema_name, matview_name, attname, comment in zip(
                comments_df["schema_name"],
                comments_df["rel_name"],
                comments_df["attname"],
                comments_df["comment"],
        ):
            if comment:
                if comment is None:
                    continue
                comment_text = f"""\nCOMMENT ON COLUMN "{schema_name}"."{matview_name}"."{attname}" IS '{comment}';\n"""
                matview_file += comment_text

        if matview_comment is not None:
            matview_file += f"""\nCOMMENT ON MATERIALIZED VIEW "{schema_name}"."{matview_name}" is '{matview_comment}';\n"""

        for index_text in indexes_df["index_text"]:
            matview_file += f"\n{index_text}\n"

        return matview_file

    def actualize_backup(self):
        res = self._connection.execute_to_list(
            get_list_of_objects.format(schemas=repr(self._schemas))
        )
        for obj, schema_name, relkind in res:
            match relkind:
                case "m":
                    ddl = self.formatting_ddl_matview(schema_name=schema_name, matview=obj)
                case "v":
                    ddl = self.formatting_ddl_view(schema_name=schema_name, view=obj)
                case _:
                    continue

            ddl_check = self._connection.execute_to_list(
                check_data, params={"schema_name": schema_name, "view_name": obj}
            )

            if not ddl_check:
                ddl_check = [("None",)]

            if ddl_check[0][0] != ddl:
                self._connection.execute(
                    insert_data,
                    params={"schema_name": schema_name, "view_name": obj, "ddl_text": ddl},
                )
                print(f"Для {schema_name}.{obj} актуализирован бэкап")

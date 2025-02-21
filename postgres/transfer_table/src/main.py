import db_sources as db
import psycopg.errors

from transfer_table.src.queries import pg, ddl


class TransferTable:
    def __init__(
            self,
            connection: [db.PostgreSQL, db.PostgreSQL],
            from_table: tuple | list,
            to_table: tuple | list,
            owner=None,
            depth: tuple | list = None

    ):
        """
        :param connection: list[0] - from db, list[1] - to db
        :param from_table: list[0] - schema, list[1] - table
        :param to_table: list[0] - schema, list[1] - table (list may be 1 length)
        """
        self._connection_from, self._connection_to = connection
        self._from_schema, self._from_table = from_table
        if len(to_table) == 1:
            self._to_schema, self._to_table = to_table[0], self._from_table
        else:
            self._to_schema, self._to_table = to_table
        if owner is None:
            self._owner = self._connection_from.execute_to_list(
                pg.get_table_owner.format(
                    table=self._from_table,
                    schema=self._from_schema
                )
            )[0][0]
        else:
            self._owner = owner

        if depth is None:
            self._depth_query = ''
            self._depth = ('Глубина не задана', 0)
        else:
            self._depth_query = pg.limit_depth.format(
                depth_column=depth[0],
                depth_months=depth[1]
            )
            self._depth = depth
        self._get_data_query = pg.get_data_query.format(
            table=self._from_table,
            schema=self._from_schema
        )

    @property
    def transfer_status(self):
        try:
            res = self._connection_to.execute_to_list(
                pg.transfer_status.format(
                    schema=self._to_schema,
                    table=self._to_table
                )
            )
            res = res[0][0]
        except psycopg.errors.UndefinedTable:
            res = None
        return res

    @property
    def source_status(self):
        try:
            res = self._connection_from.execute_to_list(
                pg.transfer_status.format(
                    schema=self._from_schema,
                    table=self._from_table
                )
            )
            res = res[0][0]
        except psycopg.errors.UndefinedTable:
            res = None
        return res

    @property
    def selection_query(self):
        return self._get_data_query + self._depth_query

    @property
    def source_ddl(self):
        res = self._connection_from.execute_to_list(
            ddl.get_ddl_query.format(
                schema=self._from_schema,
                table=self._from_table
            )
        )
        ddl_str = res[0][0]
        ddl_str = ddl_str.replace(
            f'{self._from_schema}.{self._from_table}',
            f'{self._to_schema}.{self._to_table}'
        )

        ddl_str += ddl.add_owner.format(
            table=self._to_table,
            schema=self._to_schema,
            owner=self._owner
        )
        return ddl_str

    def __str__(self):
        table = f"Исходная таблица: {self._from_schema}.{self._from_table}\n"
        to_table = f"Таблица назначения: {self._to_schema}.{self._to_table}\n"
        source_status = f"Статус исходных данных: {self.source_status}\n"
        transfer_status = f"Статус таблицы назначения: {self.transfer_status}\n"
        depth = f"Глубина переноса: {self._depth[1]} месяца, колонка с датой: {self._depth[0]}\n"
        owner = f"Назначенный владелец: {self._owner}\n"
        return table + to_table + source_status + transfer_status + depth + owner

    def transfer(self):
        if not self.source_status:
            raise AttributeError('Ошибка, источник данных отсутствует, либо таблица пустая')

        self._connection_to.execute(
            ddl.delete_data_query.format(
                schema=self._to_schema,
                table=self._to_table
            )
        )
        self._connection_to.execute(
            self.source_ddl
        )

        self._data = self._connection_from.execute_to_list(self.selection_query)
        self._connection_to.insert(
            table=self._to_table,
            schema=self._to_schema,
            values=self._data
        )
        print(f'Таблица {self._to_schema}.{self._to_table} создана и наполнена\n')

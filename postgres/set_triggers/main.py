import db_sources as db
from set_triggers.src import ddl

class TriggersDDL:

    def __init__(self, table_name: str, schema_name: str, connection: db.PostgreSQL) -> None:
        self.table_name = table_name
        self.schema_name = schema_name
        self.connection = connection
        if self.schema_name is None:
            raise Exception('schema is required')
        if self.table_name is None:
            raise Exception('table is required')
        if self.connection is None:
            raise Exception('connection required')

        self._protected = None
        self._history_trigger_exists = None
        self._history_table_exists = None


    def check_exists_trigger(self, trigger_name: str) -> bool:
        res = self.connection.execute_to_df(
            ddl.check_exiting_trigger.format(
                schema=self.schema_name,
                table=self.table_name,
                trigger=trigger_name
            )
        )
        return res.iloc[0]['exists']

    def check_exists_history(self, table_name: str) -> bool:
        res = self.connection.execute_to_df(
            ddl.check_exiting_history.format(
                schema=self.schema_name,
                table=self.table_name
            )
        )

        return res.iloc[0]['exists']

    def __str__(self):
        table_info = f"Таблица : {self.schema_name}.{self.table_name}\n"
        return table_info + self.protection

    @property
    def protection(self):
        self._protected = self.check_exists_trigger(trigger_name='lock_delete')
        info = f"Таблица защищена: {self._protected}\n"
        return info

    @protection.setter
    def protection(self, protect: bool) -> None:
        if protect and not self._protected:
            self.connection.execute(
                ddl.create_protection_trigger_ddl.format(
                    schema=self.schema_name,
                    table=self.table_name
                )
            )
            print(f"Защита от удаления поставлена {self.schema_name}.{self.table_name}\n")
        else:
            self.connection.execute(
                ddl.delete_trigger_to_protect.format(
                    schema=self.schema_name,
                    table=self.table_name
                )
            )
            print(f"Защита от удаления снята {self.schema_name}.{self.table_name}\n")


    @property
    def audit(self) -> str:
        self._history_trigger_exists = self.check_exists_trigger(trigger_name='_write_history')
        self._history_table_exists = self.check_exists_history(table_name=self.table_name)
        info = f"Историческая таблица создана: {self._history_table_exists}\n"
        info += f"Триггер стоит: {self._history_table_exists}\n"

        return info

    @audit.setter
    def audit(self, set_audit: bool) -> None:
        # if not self._history_table_exists:
        self.connection.execute(
            ddl.create_history_table.format(
                table=self.table_name,
                schema=self.schema_name
            )
        )
        self.connection.execute(
            ddl.create_comment_history_table.format(
                table=self.table_name,
                schema=self.schema_name
            )
        )
        print(f'Таблицы с историей не было, поэтому я создал:\nhistory_tables.{self.table_name}_history')


        if set_audit and not self._history_trigger_exists:
            self.connection.execute(
                ddl.create_history_trigger.format(
                    table=self.table_name,
                    schema=self.schema_name
                )
            )
            print('Триггер на аудит успешно создан')
            return
        else:
            self.connection.execute(
                ddl.delete_history_trigger.format(
                    schema=self.schema_name,
                    table=self.table_name
                )
            )
            print(f"Запись истории отключена для {self.schema_name}.{self.table_name}\n")
            return

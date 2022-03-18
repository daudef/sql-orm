from __future__ import annotations
from typing import Optional
from util.util import snake_case_to_camel_case


class ConfigImport:
    def __init__(self, module: str, object_name: str, alias: Optional[str] = None):
        self.module = module
        self.object_name = object_name
        self.alias = alias

    def name(self):
        return self.alias if self.alias is not None else self.object_name


class Config:
    class ClassName:
        parent = "SqlObject"
        entries = "Entries"

        @staticmethod
        def formater(class_name: str):
            return snake_case_to_camel_case(class_name)

    class MethodName:
        class Table:
            add_to_database = "add_to_database"
            constant = "constant"

        class Entries:
            @staticmethod
            def entry_maker(table_name: str):
                return f"make_{table_name}"

            add_all_to_database = "add_all_to_database"

    class FunctionName:
        connect_to_database = "connect_to_database"
        commit_to_database = "commit_to_database"
        close_database = "close_database"
        inner_operation = "inner"

    class VariableName:
        class AddToDatabase:
            object = "o"
            cursor = "cursor"
            statement = "stmt"
            values = "values"

        class ConnectToDataBase:
            host = "host"
            user = "user"
            password = "password"
            name = "name"

    class FieldName:
        class Entries:
            @staticmethod
            def cache(table_name: str):
                return f"_{table_name}_cache"

    class ParamName:
        connection = "connection"
        operation_input = "value"

        class AddToDatabase:
            objects = "objects"

        class ConnectToDatabase:
            host = "host"
            user = "user"
            password = "password"
            name = "database"

    class SqlConnector:
        class Imports:
            connect_function = ConfigImport(
                module="mysql.connector", object_name="connect", alias="sql_connect"
            )
            commit_function = ConfigImport(
                module="mysql.connector", object_name="commit", alias="sql_commit"
            )
            connection_type = ConfigImport(
                module="mysql.connector.connection",
                object_name="MySQLConnection",
                alias="SqlConnection",
            )

        class Methods:
            connection_new_cursor = "cursor"
            connection_commit = "commit"
            connection_close = "close"
            cursor_execute_many = "executemany"
            cursor_close = "close"

    class Message:
        abstract_method_error = "Abstract method"

    class Format:
        indent = 4 * " "

        class EmptyLines:
            after_class = 1
            after_method = 1
            after_function = 1
            after_imports = 2
            after_globals = 2

    class Semantic:
        use_primary_key_in_equality = True

    class Constant:
        default_hash = 0

#!/usr/bin/env python3

from collections import defaultdict
from json import load
from typing import Any, cast
from mysql.connector import connect as sql_connect
from python_script import PythonScript
from util.util import btos
from sql_objects import *

class Database:
    def __init__(self, host: str, password: str, user: str, schema: str) -> None:
        self.host = host
        self.password = password
        self.user = user
        self.schema = schema

    @staticmethod
    def from_json_file(json_filepath: str):
        with open(file=json_filepath, mode="r") as f:
            credentials = load(f)
        return Database(host=credentials["host"], password=credentials["password"], 
                        user=credentials["user"], schema=credentials["schema"])

    def connect(self):
        return DatabaseConnection(self)


class DatabaseConnection:
    def __init__(self, database: Database):
        self.database = database
        self.__cnx = sql_connect(host=database.host, password=database.password,
                                   user=database.user, database=database.schema)

    def execute(self, request: str, params: list[Any] = list()):
        try:
            cursor = self.__cnx.cursor()
            cursor.execute(request, params)
            results = list(cursor)
            cursor.close()
            return results
        except Exception as e:
            raise Exception(f"Request '{request} failed : {e}")

    def close(self):
        self.__cnx.close()


def get_tables(connection: DatabaseConnection):
    request = "SELECT TABLE_NAME " \
              "FROM INFORMATION_SCHEMA.TABLES " \
              "WHERE TABLE_SCHEMA = %s"
    params = [connection.database.schema]
    results: list[tuple[str]] = connection.execute(request = request, params=params)
    return list(SqlTable(name=x[0]) for x in results)

def get_table_columns(connection: DatabaseConnection):
    requested_columns = ["TABLE_NAME", "COLUMN_NAME", "IS_NULLABLE", "DATA_TYPE", "COLUMN_TYPE", 
                         "CHARACTER_MAXIMUM_LENGTH", "NUMERIC_PRECISION", "COLUMN_DEFAULT"]
    request = "SELECT " + ", ".join(requested_columns) + " " \
              "FROM INFORMATION_SCHEMA.COLUMNS " \
              "WHERE TABLE_SCHEMA = %s"
    params = [connection.database.schema]
    results: list[tuple[str, str, str, str, bytes, int, int, bytes]] = \
        connection.execute(request = request, params=params)
    return list((table_name, 
        SqlColumn(name=column_name, 
                  data_type=make_type(data_type=data_type, column_type=column_type,
                                 char_limit=char_limit, precision=precision), 
                  optional=nullable == "YES",
                  default=default)) 
        for (table_name, column_name, nullable, data_type, column_type, char_limit, precision, default) 
            in results)

def get_constraints(connection: DatabaseConnection, constraint_type: str) -> list[tuple[str, SqlUniqueConstraint]]:
    requested_columns = ["KCU.CONSTRAINT_NAME", "KCU.TABLE_NAME", "KCU.COLUMN_NAME", 
                         "KCU.REFERENCED_TABLE_NAME", "KCU.REFERENCED_COLUMN_NAME"]

    request = "SELECT " + ", ".join(requested_columns) + " " \
              "FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE as KCU, INFORMATION_SCHEMA.TABLE_CONSTRAINTS as TC " \
              "WHERE KCU.CONSTRAINT_SCHEMA = %s " \
              "AND TC.TABLE_SCHEMA = KCU.TABLE_SCHEMA " \
              "AND TC.CONSTRAINT_NAME = KCU.CONSTRAINT_NAME " \
              "AND TC.TABLE_NAME = KCU.TABLE_NAME " \
              "AND TC.CONSTRAINT_TYPE = %s"

    params = [connection.database.schema, constraint_type]

    results: list[tuple[str, str, str, str, str]] = connection.execute(request = request, params=params)
    raw_constraints: dict[tuple[str, str, str], tuple[list[str], list[str]]] = defaultdict(lambda: ([],[]))
    for (c_name, t_name, col_name, reft_name, refcol_name) in results:
        raw_constraints[(c_name, t_name, reft_name)][0].append(col_name)
        raw_constraints[(c_name, t_name, reft_name)][1].append(refcol_name)
    if constraint_type == SqlUniqueConstraint.type():
        return list((t_name, SqlUniqueConstraint(column_names=col_names))
                    for (_, t_name, _), (col_names, _) in raw_constraints.items())
    elif constraint_type == SqlPrimaryConstraint.type():
        return list((t_name,SqlPrimaryConstraint(column_names=col_names))
                    for (_, t_name, _), (col_names, _) in raw_constraints.items())
    elif constraint_type == SqlForeignConstraint.type():
        return list((t_name, SqlForeignConstraint(column_names=col_names, 
                                                  referenced_table_name=reft_name, 
                                                  referenced_column_names=refcol_names))
                    for (_, t_name, reft_name), (col_names, refcol_names) 
                        in raw_constraints.items())
    else:
        raise Exception(f"Unknown constraint type {constraint_type}")


def make_type(data_type: str, column_type: bytes, char_limit: int, precision: int) -> SqlType:
    if data_type == "datetime":
        return SqlDateTimeType()
    elif data_type == "enum":
        values = list(v for i, v in enumerate(btos(column_type).split("'")) if i % 2 == 1)
        return SqlEnumType(values=values)
    elif data_type == "float":
        return SqlFloatType(precision=precision)
    elif data_type == "int":
        return SqlIntegerType(precision=precision)
    elif data_type == "json":
        return SqlJsonType()
    elif data_type == "longblob":
        return SqlBlobType(char_limit=char_limit)
    elif data_type == "smallint":
        return SqlIntegerType(precision=precision)
    elif data_type == "time":
        return SqlTimeType()
    elif data_type == "tinyint":
        return SqlIntegerType(precision=precision)
    elif data_type == "varchar":
        return SqlStringType(char_limit=char_limit)
    else:
         raise Exception(f"Unknown datatype {data_type}")

def generate_schema(database: Database):
    connection = database.connect()

    tables = {table.name: table for table in get_tables(connection)}
    for (table_name, column) in get_table_columns(connection):
        tables[table_name].add_column(column)

    for (t_name, unique_constraint) in get_constraints(connection=connection, 
            constraint_type=SqlUniqueConstraint.type()):
        tables[t_name].add_unique_constraint(unique_constraint=unique_constraint)

    for (t_name, primary_constraint) in get_constraints(connection=connection, 
            constraint_type=SqlPrimaryConstraint.type()):
        primary_constraint = cast(SqlPrimaryConstraint, primary_constraint)
        tables[t_name].add_primary_constraint(primary_constraint=primary_constraint)

    foreign_constraints = get_constraints(connection=connection,
            constraint_type=SqlForeignConstraint.type())
    while (any(tables[t_name].add_foreign_constraint(
                foreign_constraint=cast(SqlForeignConstraint, fc), 
                referred_table=tables[cast(SqlForeignConstraint, fc).referenced_table_name]) 
                for (t_name, fc) in foreign_constraints)):
        pass

    connection.close()
    return tables

def pretty_print_tables(tables: dict[str, SqlTable]):
    for table in tables.values():
        table.pretty_print()
        print(3 * "\n", end="")


def gen_connect_to_database_function(parent_script: PythonScript, database: Database):
    with parent_script.gen_function_decl(function_name=Config.FunctionName.connect_to_database) as script:
        script.imports.add_import_from_config(Config.SqlConnector.Imports.connect_function)
        host_var = PythonVariable(name=Config.VariableName.ConnectToDataBase.host, 
                    type=StrPythonType(), initial_litteral=database.host)
        user_var = PythonVariable(name=Config.VariableName.ConnectToDataBase.user, 
                    type=StrPythonType(), initial_litteral=database.user)
        password_var = PythonVariable(name=Config.VariableName.ConnectToDataBase.password, 
                    type=StrPythonType(), initial_litteral=database.password)
        schema_var = PythonVariable(name=Config.VariableName.ConnectToDataBase.schema, 
                    type=StrPythonType(), initial_litteral=database.schema)
        host_var.gen_declarartion(script=script, with_typing=False)
        user_var.gen_declarartion(script=script, with_typing=False)
        password_var.gen_declarartion(script=script, with_typing=False)
        schema_var.gen_declarartion(script=script, with_typing=False)
        script.add_aligned_line(start=f"return {Config.SqlConnector.Imports.connect_function.name()}(",
                    end=")", separator=", ", values=[
                        f"{Config.ParamName.ConnectToDatabase.host}={host_var}",
                        f"{Config.ParamName.ConnectToDatabase.user}={user_var}",
                        f"{Config.ParamName.ConnectToDatabase.password}={password_var}",
                        f"{Config.ParamName.ConnectToDatabase.schema}={schema_var}"])

def gen_commit_to_database_function(parent_script: PythonScript):
    parent_script.imports.add_import_from_config(Config.SqlConnector.Imports.connection_type)
    cnx_param = PythonVariable(name=Config.ParamName.connection, 
                type=ClassPythonType(Config.SqlConnector.Imports.connection_type.name()))
    with parent_script.gen_function_decl(function_name=Config.FunctionName.commit_to_database, 
                params=[cnx_param]) as script:
        script.add_line(f"{cnx_param}.{Config.SqlConnector.Methods.connection_commit}()")    
    
def gen_close_database_function(parent_script: PythonScript):
    parent_script.imports.add_import_from_config(Config.SqlConnector.Imports.connection_type)
    cnx_param = PythonVariable(name=Config.ParamName.connection, 
                type=ClassPythonType(Config.SqlConnector.Imports.connection_type.name()))
    with parent_script.gen_function_decl(function_name=Config.FunctionName.close_database, 
                params=[cnx_param]) as script:
        script.add_line(f"{cnx_param}.{Config.SqlConnector.Methods.connection_close}()") 


def gen_entries_class(parent_script: PythonScript, tables: dict[str, SqlTable]):
    with parent_script.gen_class_decl(ClassPythonType(Config.ClassName.entries)) as script:
        caches: dict[str, PythonVariable] = dict()

        with script.gen_method_decl(method_name="__init__") as ctx_script:
            for table in tables.values():
                table_cache = PythonField(name=Config.FieldName.Entries.cache(table_name=table.name), 
                        type=DictPythonType(table.class_type, table.class_type), initial_litteral={})
                table_cache.gen_declarartion(script=ctx_script)
                caches[table.name] = table_cache

        cnx_param = SqlTable.get_connection_param(imports=script.imports)
        with script.gen_method_decl(method_name=Config.MethodName.Entries.add_all_to_database, 
                    params=[cnx_param]) as aatd_script:
            for table in tables.values():
                aatd_script.add_line(f"{table.class_type.gen_type(imports=script.imports)}." \
                    f"{Config.MethodName.Table.add_to_database}" \
                    f"({Config.ParamName.AddToDatabase.objects}={caches[table.name]}, "
                    f"{cnx_param}={cnx_param})")
            
        for table in tables.values():
            with script.gen_method_decl(method_name=Config.MethodName.Entries.entry_maker(table.name), 
                        params=table.field_variables()) as ct_script:
                ct_var = PythonVariable(name=table.name, type=table.class_type)
                ct_script.add_aligned_line(separator=", ", 
                            values=(f"{f}={f}" for f in table.field_variables()),
                            start=f"{ct_var} = {table.class_type.gen_type(imports=ctx_script.imports)}(",
                            end=")")
                with ct_script.gen_if(f"{ct_var} not in {caches[table.name]}") as inc_script:
                    inc_script.add_line(f"{caches[table.name]}[{ct_var}] = {ct_var}")
                ct_script.add_line(f"return {caches[table.name]}[{ct_var}]")

def generate_objects_code(script: PythonScript, tables: dict[str, SqlTable], database: Database):
    gen_connect_to_database_function(parent_script=script, database=database)
    gen_commit_to_database_function(parent_script=script)
    gen_close_database_function(parent_script=script)

    gen_entries_class(parent_script=script, tables=tables)
    
    SqlTable.gen_parent_method(parent_script=script)
    for table in tables.values():
        table.generate(script=script)
    return script


def generate_usage_template_code(script: PythonScript, tables: dict[str, SqlTable], objects_path: str):
    script.imports.add_import(module=objects_path.replace("/", ".").replace(".py", ""), object="*")

    entries_var = PythonVariable(name="entries", type=ClassPythonType(Config.ClassName.entries),
                                 initial_litteral=[])
    entries_var.gen_declarartion(script=script, with_typing=False)

    script.add_line(f"# Do stuff with {entries_var}")

    script.add_line(f"{Config.ParamName.connection} = {Config.FunctionName.connect_to_database}()")
    with script.gen_try() as try_script:
        try_script.add_line(f"{entries_var}.{Config.MethodName.Entries.add_all_to_database}" \
                            f"({Config.ParamName.connection}={Config.ParamName.connection})")
        try_script.add_line(f"{Config.FunctionName.commit_to_database}({Config.ParamName.connection})")
    with script.gen_except() as except_script:
        except_script.add_line("pass")
    script.add_line(f"{Config.FunctionName.close_database}({Config.ParamName.connection})")


    return script




def main():
    database=Database.from_json_file(json_filepath="credentials.json")
    output_objects_filepath = "gen/objects.py"
    output_usage_template_filepath = "gen/usage_template.py"

    tables = generate_schema(database)
    pretty_print_tables(tables=tables)

    script = PythonScript()
    objects_script = generate_objects_code(script=script, tables=tables, database=database)
    with open(file=output_objects_filepath, mode="w") as f:
        f.write(objects_script.get_script())

    script = PythonScript()
    usage_script = generate_usage_template_code(script=script, tables=tables, 
                objects_path=output_objects_filepath)
    with open(file=output_usage_template_filepath, mode="w") as f:
        f.write(usage_script.get_script())


if __name__ == "__main__":
    main()
             

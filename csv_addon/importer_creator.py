#!/usr/bin/env python3
from __future__ import annotations

try:
    import code_gen_config #type: ignore
except ModuleNotFoundError:
    import sys
    sys.path[0] += "/.."

from sys import argv
from collections import defaultdict
from code_gen_config import Config
from csv_addon.csv_profile import CsvProfile 
from objects_generator import generate_entries_commit, generate_objects_code, generate_schema
from python_script import ClassPythonType, DictPythonType, PythonScript, PythonVariable, StrPythonType
from csv_addon.importer_code_gen_config import ImportConfig
from sql_objects import SqlTables, TableClassPythonType
from itertools import chain
from database import Database, choose_database



def get_profile_path():
    if len(argv) != 2:
        print(f"Usage: {argv[0]} <path_to_profile>")
        exit()
    return argv[1]


def gen_import_loop(script: PythonScript, table_column_refs: dict[str, dict[str, PythonVariable]], 
                row_var: PythonVariable, entries_var: PythonVariable, profile: CsvProfile):
    used_t_names = set(chain.from_iterable(column_ref for column_ref in table_column_refs.values()))
    for m in profile:
        t_name = m.table.name
        table_object_var = PythonVariable(ImportConfig.VariableName.table_object(t_name))
        m.gen_row_converter_call(script, table_column_refs[t_name], entries_param=entries_var, 
                    row_param=row_var, prefix=f"{table_object_var} = " if t_name in used_t_names else "")
    

def gen_import_main(script: PythonScript, table_object_vars: dict[str, dict[str, PythonVariable]],
                row_var: PythonVariable, entries_var: PythonVariable, profile: CsvProfile):
    with script.gen_function_decl(ImportConfig.FunctionName.import_csv):
        script.imports.add_import(module="sys", object="argv")
        with script.gen_if("len(argv) != 2"):
            script.add_line('print(f"Usage: {argv[0]} <path_to_csv>")')
            script.add_line("exit()")
        csv_file_var = PythonVariable(name=ImportConfig.VariableName.csv_file)
        entries_var.gen_declarartion(script, with_typing=False)

        with script.gen_with(f'open(file=argv[1], mode="r")', csv_file_var):
            script.imports.add_import("csv", "DictReader")
            with script.gen_for(variables=[row_var], iterable=f'DictReader({csv_file_var}, delimiter=";")'):
                gen_import_loop(script, table_object_vars, row_var=row_var, entries_var=entries_var, 
                                profile=profile)
        generate_entries_commit(script, entries_var)

def gen_import_functions(script: PythonScript, profile: CsvProfile, tables: SqlTables):
    profile.gen_operations(script)

    table_object_vars = make_table_object_vars(profile, tables)
    
    entries_var = PythonVariable(name=ImportConfig.VariableName.entries, 
                                 type=ClassPythonType(Config.ClassName.entries))
    row_var = PythonVariable(name=ImportConfig.VariableName.csv_row, 
                               type=DictPythonType(StrPythonType(), StrPythonType()))
    for m in profile:
        m.gen_row_converter_decl(script, table_object_vars[m.table.name], entries_param=entries_var, 
                    row_param=row_var)
    gen_import_main(script, entries_var=entries_var, row_var=row_var, profile=profile,
                table_object_vars=table_object_vars)

    with script.gen_if(f'__name__ == "__main__"'):
        script.add_line(f"{ImportConfig.FunctionName.import_csv}()")


def make_table_object_vars(profile: CsvProfile, tables: SqlTables):
    previous_t_names: set[str] = set()
    table_object_vars: dict[str, dict[str, PythonVariable]] = defaultdict(dict)
    for m in profile:
        for c in m.table.columns.values():
            if c.data_type.is_referrence():
                rt_name = c.data_type.get_reffered_table_name()
                if rt_name in previous_t_names:
                    table_object_vars[m.table.name][rt_name] = PythonVariable(
                                name=c.name, 
                                type=TableClassPythonType(tables[rt_name]))
        previous_t_names.add(m.table.name)
    return table_object_vars
        



def filter_imported_table(profile: CsvProfile, tables: SqlTables):
    imported_t_name = set(tables.child_names_of((m.table.name for m in profile)))
    for t_name in list(tables):
        if t_name not in imported_t_name:
            del tables[t_name]

def get_importer_path(profile_path: str):
    profile_suffix = "_profile.json"
    assert(profile_path[-len(profile_suffix):] == profile_suffix)
    return profile_path[:-len(profile_suffix)] + "_importer.py"


def create_importer(database: Database, profile: CsvProfile):
    tables = generate_schema(database)
    filter_imported_table(profile, tables)
    script = PythonScript()
    generate_objects_code(script, tables, database)
    gen_import_functions(script, profile, tables)
    return script

if __name__ == "__main__":
    database = choose_database()
    tables = generate_schema(database)
    with open("gen/import.py", mode="w") as f:
        f.write(create_importer(database=database, profile=CsvProfile.from_json_file(filepath=get_profile_path(), tables=tables)).get_script())
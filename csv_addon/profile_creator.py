#!/usr/bin/env python3

from __future__ import annotations
import sys
sys.path[0] += "/.."

from csv import DictReader
from typing import Iterable, Optional
from operations import OperationChain
from os import get_terminal_size
from objects_generator import Database, generate_schema
from sql_objects import SqlReferenceType, SqlTable
from util.graph import Graph
from util.util import print_choose, str_similar, yes_no_choose
from csv_addon.csv_profile import CSV_NO_COLUMN, CSV_OPERATIONS, Constant, NormalCsv, TableProfile
from python_script import DateTimePythonType, EnumPythonType, FloatPythonType, IntPythonType, NonePythonType, PythonScript, PythonType, StrPythonType, TimePythonType
from collections import defaultdict

def get_suffix():
    return "\n" + "#" * get_terminal_size()[0] + "\n\n"

def get_prefix():
    return "\n --- \n"

TABLE_CHOICE_QUIT = "I'm done"
NORMAL_CSV_METHOD_NAME = "Normal CSV"
CONSTANT_METHOD_NAME = "Constant"

AUTO_DETECTED_TYPES: list[PythonType] = [
    StrPythonType(),
    IntPythonType(),
    FloatPythonType(),
    DateTimePythonType(),
    TimePythonType(),
]

ENUM_AUTO_DETECT_LIMIT = 10
OPTIONAL_VALUES = [" ", ]

def is_optional_value(s: str):
    return len(s.strip()) == 0


def create_table_profile():
    pass


METHODS_NAME = {
    NORMAL_CSV_METHOD_NAME: NormalCsv,
    CONSTANT_METHOD_NAME : Constant,
}


def create_table_method_names(tables: dict[str, SqlTable]):
    table_method_names: dict[str, str] = dict()
    available_table_choices = set(tables)
    def explorer(t_name: str, optional: bool = False):
        for column in tables[t_name].columns.values():
            data_type = column.data_type
            if isinstance(data_type, SqlReferenceType) and optional == column.optional:
                yield data_type.table.name
    graph = Graph(nodes=list(tables), explorer=explorer) # type: ignore
    while len(table_method_names) == 0 or print_choose(items=["Yes", "No"], printer=lambda x: x, 
                prompt="Import another table ?", suffix=get_suffix(), default="Yes", prefix=get_prefix()) == "Yes":
        available_table_choices = list(sorted(t_name for t_name in tables if t_name not in table_method_names))
        default = None
        if len(table_method_names) > 0:
            default = TABLE_CHOICE_QUIT
        choosed_table_name = print_choose(items=list(available_table_choices), printer=lambda x: x,
                    prompt="Choose table import", suffix=get_suffix(), default=default, prefix=get_prefix())
        if choosed_table_name == TABLE_CHOICE_QUIT:
            break
        original_table_name = choosed_table_name
        while True:
            def validate_table_name(t_name: str, ot_name: Optional[str]) -> bool:
                if ot_name is not None:
                    print(f"Required by {ot_name}: {t_name}")
                if t_name in table_method_names:
                    print("Already filled")
                    return False
                items = list(sorted(m_name for (m_name, m) in METHODS_NAME.items() 
                            if m.write_to_database() or t_name != original_table_name))
                choosed_method_name = print_choose(items=items, printer=lambda x: x, default=NORMAL_CSV_METHOD_NAME,
                            suffix=get_suffix(), prompt=f"Choose generation method for {t_name}",
                            prefix=get_prefix())
                print(f"Using {choosed_method_name} for {t_name}")
                table_method_names[t_name] = choosed_method_name
                return METHODS_NAME[choosed_method_name].is_recursive()
            list(graph.reachable_nodes_from(node=choosed_table_name, validator=validate_table_name)) # type: ignore
            no_optional = "No optional table"
            optional_deps = list(sorted(set(explorer(original_table_name, optional=True)).difference(table_method_names)))
            choosed_table_name = print_choose(items=optional_deps, printer=lambda x:x, prefix=get_prefix(),
                        prompt="Choose optional table to import", suffix=get_suffix(),
                        default=no_optional)
            if choosed_table_name == no_optional:
                break
        print("\n --- Summary ---")
        for (t_name, method) in table_method_names.items():
            print(f"    - import {t_name} with {method}")
    
    return table_method_names


def create_normal_csv_profile(method: NormalCsv, field_names: str, 
            field_name_type: dict[str, set[PythonType]]):
    req_columns = set(c.name for c in method.required_columns())
    opt_colums = set(c.name for c in method.optional_columns())

    if len(method.table.get_primary_column_names()) == 1 and \
                yes_no_choose(prompt="Auto-generate primary key ?"):
        generator = print_choose(
            items=list(o for o in CSV_OPERATIONS if o.primary and o.input_type.is_super_type(NonePythonType())),
            printer=lambda o: o.name,
            prompt="Choose generator")
        primary_column = method.table.columns[method.table.get_primary_column_names()[0]]
        method.table_profile[primary_column] = OperationChain([CSV_NO_COLUMN, generator])
        req_columns.remove(primary_column.name)

    field_column_association: dict[str, Optional[str]] = defaultdict(lambda: None)

    if len(req_columns) == 1:
        for f_name in field_names:
            if str_similar(f_name, method.table.name):
                c_name = list(req_columns)[0]
                field_column_association[c_name] = f_name
                req_columns.remove(c_name)

    for c_name in method.table.columns:
        for f_name in field_names:
            pass




def create_profile(tables: dict[str, SqlTable], table_method_names: dict[str, str], 
            field_name_type: dict[str, DetectedType]):
    def explorer(t_name: str):
        for column in tables[t_name].columns.values():
            data_type = column.data_type
            if isinstance(data_type, SqlReferenceType) and \
                        (not column.optional or data_type.table.name in table_method_names):
                yield data_type.table.name
    graph = Graph(nodes=list(tables), explorer=explorer) # type: ignore
    methods = list(METHODS_NAME[table_method_names[t_name]](tables[t_name], TableProfile({})) #type: ignore
            for t_name in graph.sink_to_source_exploration() if t_name in table_method_names) # type: ignore

    
        
    for method in methods:
        pass

            

        
        

def get_csv_filepath():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <path_to_csv>")
        exit()
    return sys.argv[1]

class DetectedType:
    def __init__(self, types: Iterable[PythonType], optional: bool = False,
                 values: Optional[set[str]] = None) -> None:
        self.types = set(types)
        self.optional = optional
        self.values: Optional[set[str]] = set() if values is None else values
        self.enum_type = EnumPythonType(list(self.values))
        self.types.add(self.enum_type)

    @staticmethod
    def default():
        return DetectedType(types=AUTO_DETECTED_TYPES)

    def infer_from_value(self, value: str):
        if is_optional_value(value):
            self.optional = True
            return

        for type in list(self.types):
            if not type.is_valid_str_litteral(value) and type != self.enum_type:
                self.types.remove(type)

        if self.values is not None:
            if value not in self.values:
                self.values.add(value)
                self.enum_type.values.append(value)
            if len(self.values) > ENUM_AUTO_DETECT_LIMIT:
                self.types.remove(self.enum_type)
                self.values = None

    def __str__(self):
        types_str = " | ".join(t.gen_type(imports=PythonScript().imports) for t in self.types)
        if self.optional:
            types_str = f"Optional[{types_str}]"
        return types_str



def get_field_name_type(csv_filepath: str):
    delimiter = ","
    with open(file=csv_filepath) as csv_file:
        for line in csv_file:
            if line.count(";") > line.count(","):
                delimiter = ";"
            break

    
    with open(file=csv_filepath) as csv_file:
        csv_reader = DictReader(csv_file, delimiter=delimiter)
        if csv_reader.fieldnames is None:
            raise Exception("Invalid File : no header")
        field_name_type: dict[str, DetectedType] = \
                        {f_name: DetectedType.default() for f_name in csv_reader.fieldnames}
        if len(csv_reader.fieldnames) <= 1:
            if not yes_no_choose(prompt="CSV file contains only one column '" + \
                            csv_reader.fieldnames[0] + "', do you want to continue ?"):
                    raise Exception("Invalid File: (maybe) wrong separator")
        for (i, row) in enumerate(csv_reader):
            if None in row or any(v is None for v in row.values()):
                Exception(f"Invalid File : at line {i}, wrong number of values")
            for (k, v) in row.items():
                field_name_type[k].infer_from_value(v)
        return field_name_type

                


        

def generate_profile(database: Database):
    csv_filepath = get_csv_filepath()
    field_name_type = get_field_name_type(csv_filepath=csv_filepath)
    for (f_name, types) in field_name_type.items():

        print(f"{f_name}: {types}")



    tables = generate_schema(database=database)
    table_method_type_names = create_table_method_names(tables=tables)
    create_profile(tables=tables, table_method_names=table_method_type_names, field_name_type=field_name_type)


if __name__ == "__main__":
    generate_profile(database=Database.from_json_file("credentials.json"))

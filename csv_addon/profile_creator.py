#!/usr/bin/env python3

from __future__ import annotations


try:
    import csv_addon.csv_profile #type: ignore
except ModuleNotFoundError:
    import sys
    sys.path[0] += "/.."

from csv import DictReader
from csv_addon.util import find_csv_delimiter
from database import choose_database
from typing import Callable, Iterable, Optional, TypeVar, cast
from operations import CONSTANT_STR_OP, ENUM_CONVERTER_OP, NULLIFY_STR_OP, TO_ASCII_OP, UPPER_STR_OP, Operation, OperationChain
from os import get_terminal_size
from objects_generator import Database, generate_schema
from sql_objects import SqlColumn, SqlReferenceType, SqlStringType, SqlTable
from util.graph import Graph
from util.util import choose, free_input, print_choose, str_similarity, yes_no_choose
from operation_functions import nullify_str, to_ascii, upper_str
from csv_addon.csv_profile import *
from python_script import *
from collections import defaultdict
from math import inf

def get_suffix():
    return "\n" + "#" * get_terminal_size()[0] + "\n\n"

def get_prefix():
    return "\n --- \n"

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


METHODS_NAME = {
    NORMAL_CSV_METHOD_NAME: NormalCsv,
    CONSTANT_METHOD_NAME : Constant,
}


T = TypeVar("T")
def this_gen_print_choose(items: list[T], prompt: str, printer: Callable[[T], str], default: Optional[T] = None):
    return print_choose(items=items, printer=printer, prompt=prompt, suffix=get_suffix(), 
                prefix=get_prefix(), default=default)

def this_print_choose(items: list[str], prompt: str, default: Optional[str] = None, 
            printer: Callable[[str], str] = lambda s:s):
    return print_choose(items=items, printer=printer, prompt=prompt, suffix=get_suffix(), 
                prefix=get_prefix(), default=default)

def this_yes_no_choose(prompt: str, default: Optional[bool] = None):
    return yes_no_choose(prompt=prompt, suffix=get_suffix(), prefix=get_prefix(), default=default)

def this_free_choose(prompt: str, default: Optional[str] = None, validator: Callable[[str], bool] = lambda s:True, invalid_message: Optional[str] = None):
    if invalid_message is None:
        return free_input(prompt=prompt, prefix=get_prefix(), suffix=get_suffix(), default=default, 
                    validator=validator)
    else:
        return free_input(prompt=prompt, prefix=get_prefix(), suffix=get_suffix(), default=default, 
                    validator=validator, invalid_message=invalid_message)


def create_table_method_names(tables: dict[str, SqlTable], field_name_type: dict[str, DeducedType]):
    table_method_names: dict[str, str] = dict()
    available_table_choices = set(tables)
    def explorer(t_name: str, optional: bool = False):
        for column in tables[t_name].columns.values():
            data_type = column.data_type
            if isinstance(data_type, SqlReferenceType) and optional == column.optional:
                yield data_type.table.name
    graph = Graph(nodes=list(tables), explorer=explorer) # type: ignore
    
    while len(table_method_names) == 0 or yes_no_choose(prompt="Import another table ?", prefix=get_prefix(), suffix=get_suffix(), default=True):
        available_table_choices = list(sorted(t_name for t_name in tables if t_name not in table_method_names))
        default = None

        table_choose_quit = "I'm done"
        table_choose_auto = "Auto-detect tables"

        if len(table_method_names) > 0:
            default = table_choose_quit
        else: 
            default = table_choose_auto
        choosed_name = print_choose(items=list(available_table_choices), printer=lambda x: x,
                    prompt="Choose table to import", suffix=get_suffix(), default=default, prefix=get_prefix())
        if choosed_name == table_choose_quit:
            break

        if choosed_name == table_choose_auto:
            hesitated = False
            root_t_names: set[str] = set()
            t_name_column: list[tuple[str, SqlColumn]] = list()
            for table in tables.values():
                for column in table.columns.values():
                    t_name_column.append((table.name, column))
            f_name_t_names_columns: dict[str, dict[str, list[SqlColumn]]] = dict()
            for (f_name, f_type) in field_name_type.items():
                t_name_columns, _ = find_columns_for_field(f_name=f_name, f_type=f_type, 
                            t_name_column=t_name_column)
                f_name_t_names_columns[f_name] = t_name_columns
                if len(t_name_columns) == 1:
                    root_t_names.add(list(t_name_columns.keys())[0])
                else:
                    hesitated = True
                    print(f"{f_name}: hesitating between {', '.join(', '.join(f'{t_name}.{c.name}' for c in cs) for (t_name, cs) in t_name_columns.items())}")
            print()
            print("Auto-detected tables:")
            print("\n".join(f"   - {n} (for field(s) {', '.join(f_name for (f_name, t_name_columns) in f_name_t_names_columns.items() if len(t_name_columns) == 1 and list(t_name_columns)[0] == n)})" for n in root_t_names))
            if hesitated:
                print("\n(Tip: to improve auto-detection, put the name of the table in the name of the field)")
            if not yes_no_choose(prompt="Do you want to import all those tables", default=True, 
                        prefix=get_prefix(), suffix=get_suffix()):
                continue
        else:
            root_t_names = {choosed_name}

        choosed_t_names = list(root_t_names)

        while True:
            def validate_table_name(t_name: str, ot_name: Optional[str]) -> bool:
                if ot_name is not None:
                    print(f"Required by {ot_name}: {t_name}")
                if t_name in table_method_names:
                    print("Already filled")
                    return False
                items = list(sorted(m_name for (m_name, m) in METHODS_NAME.items() 
                            if m.write_to_database() or t_name not in root_t_names))
                choosed_method_name = print_choose(items=items, printer=lambda x: x, default=NORMAL_CSV_METHOD_NAME,
                            suffix=get_suffix(), prompt=f"Choose generation method for {t_name}",
                            prefix=get_prefix())
                print(f"Using {choosed_method_name} for {t_name}")
                table_method_names[t_name] = choosed_method_name
                return METHODS_NAME[choosed_method_name].is_recursive()
            list(graph.reachable_nodes_from(nodes=choosed_t_names, validator=validate_table_name)) # type: ignore
            no_optional = "No optional table"
            optional_tables: set[str] = set()
            for t_name in root_t_names:
                optional_tables.update(explorer(t_name, optional=True))
            optional_deps = list(sorted(optional_tables.difference(table_method_names)))
            choosed_name = print_choose(items=optional_deps, printer=lambda x:x, prefix=get_prefix(),
                        prompt="Choose optional table to import", suffix=get_suffix(),
                        default=no_optional)
            choosed_t_names = [choosed_name]    
            if choosed_name == no_optional:
                break
        print("\n --- Summary ---")
        for (t_name, method) in table_method_names.items():
            print(f"    - import {t_name} with {method}")
    
    return table_method_names


def find_field_for_column(column: SqlColumn, t_name: str, field_names: Iterable[str], field_name_type: dict[str, DeducedType]):
    best_similarity = -inf
    best_f_name = None
    c_name = t_name + "." + column.name
    for f_name in field_names:
        if field_name_type[f_name].is_assignable_to(column.make_type()):
            similarity = str_similarity(f_name, c_name)
            if similarity > best_similarity:
                best_similarity = similarity
                best_f_name = f_name
    return best_f_name, best_similarity


def find_columns_for_field(f_name: str, f_type: DeducedType, t_name_column: list[tuple[str, SqlColumn]]):
    best_similarity = -inf
    best_t_name_columns: dict[str, list[SqlColumn]] = defaultdict(list)
    for (t_name, column) in t_name_column:
        c_name = t_name + "." + column.name
        try:
            similarity = str_similarity(f_name, c_name)
            if f_type.is_assignable_to(column.make_type()):
                similarity += 2
            if similarity == best_similarity:
                best_t_name_columns[t_name].append(column)
            elif similarity > best_similarity:
                best_t_name_columns.clear()
                best_t_name_columns[t_name] = [column]
                best_similarity = similarity

        except Exception:
            pass
    return best_t_name_columns, best_similarity


def make_csv_column_instance(f_name: str, field_name_type: dict[str, DeducedType]):
    f_type = field_name_type[f_name]
    if f_type.values is None:
        return CSV_COLUMN_OP.make_instance([f_name])
    else:
        return CSV_ENUM_COLUMN_OP.make_instance(arg_values=[f_name], 
                dynamic_output_type=EnumPythonType(StrPythonType(),
                    list(f_type.values) + [""] if f_type.optional else []))


def set_default_csv_operation_chain(method: NormalCsv, column: SqlColumn, f_name: str, 
            field_name_type: dict[str, DeducedType]):
    chain = OperationChain(operations=[make_csv_column_instance(f_name, field_name_type)])

    if column.data_type == SqlStringType(0) and  method.table.is_in_unique(column.name):
        chain.append(TO_ASCII_OP.make_instance())
        chain.append(UPPER_STR_OP.make_instance())
    c_type = column.make_type()

    if field_name_type[f_name].optional and c_type != StrPythonType():
        chain.append(NULLIFY_STR_OP.make_instance())

    if not c_type.is_text() or not field_name_type[f_name].is_assignable_to(c_type):
        for op in CSV_OPERATIONS.values():
            if op.get_input_type().is_super_type(chain.last_type()) \
                        and column.make_type().is_super_type(op.get_output_type()):
                chain.append(op.make_default_instance())
                break
        else:
            print(f"Error: No default converter from type {field_name_type[f_name]} to type {column.make_type()}")
            print(f"{method.table.name=}")
            print(f"{column=}")
            print(f"{f_name=}")
            raise Exception()
    method.table_profile[column] = chain
                            


def create_normal_csv_profile(methods: list[NormalCsv], field_name_type: dict[str, DeducedType]):
    methods_table = {m.table.name: m for m in methods}
    table_req_column_names = {m.table.name: set(c.name for c in m.required_columns()) for m in methods}
    table_opt_colum_names = {m.table.name: set(c.name for c in m.optional_columns()) for m in methods}

    # Auto generating primary keys
    t_name_generable_pk = {m.table.name for m in methods 
                if len(list(0 for c_name in m.table.get_primary_column_names() 
                        if c_name in table_req_column_names[m.table.name])) == 1}
    while True:
        if len(t_name_generable_pk) <= 1:
            break
        exit_choice = "All of those table's ids are auto-generated"
        choosed_table = this_print_choose(items=list(t_name_generable_pk), default=exit_choice, 
                    prompt="Choose table whose id is *NOT* auto-generated")
        if choosed_table == exit_choice:
            break
        t_name_generable_pk.remove(choosed_table)

    for t_name in t_name_generable_pk:
        table = methods_table[t_name].table
        primary_column = table.columns[table.get_primary_column_names()[0]]
        generators = list(o for o in CSV_OPERATIONS.values() if o.primary
            and o.get_input_type().is_super_type(NonePythonType())
            and primary_column.make_type().is_super_type(o.get_output_type()))
        if len(generators) == 0:
            print(f"Error: no generator available for {t_name}, continuing")
            continue
        generator = print_choose(printer=lambda o: o.name, prompt="Choose generator", 
                    items=generators, suffix=get_suffix(), prefix=get_prefix())
        print(f"Using {generator.name} for {t_name}")
        methods_table[t_name].table_profile[primary_column] = \
                    OperationChain([CSV_NO_COLUMN_OP.make_instance(), generator.make_instance()])
        table_req_column_names[t_name].remove(primary_column.name)

    # Finding best fields for required columns
    non_assigned_fields = set(field_name_type)
    for (t_name, req_column_names) in table_req_column_names.items():
        for c_name in req_column_names:
            column = methods_table[t_name].table.columns[c_name]
            f_name, _ = find_field_for_column(column=column, t_name=t_name, 
                        field_names=field_name_type, field_name_type=field_name_type)
            if f_name is not None:
                set_default_csv_operation_chain(method=methods_table[t_name], column=column, 
                            f_name=f_name, field_name_type=field_name_type)
                non_assigned_fields.discard(f_name)

    # Finding best optional columns for available fields
    assigned_table_opt_column_names: dict[str, set[str]] = defaultdict(set)
    while True:
        best_t_name = None
        best_c_name = None
        best_f_name = None
        best_similarity = -inf
        for (t_name, opt_column_names) in table_opt_colum_names.items():
            for c_name in opt_column_names:
                if c_name in assigned_table_opt_column_names[t_name]:
                    continue
                column = methods_table[t_name].table.columns[c_name]
                f_name, similarity = \
                    find_field_for_column(column=column, t_name=t_name, 
                                field_names=non_assigned_fields, field_name_type=field_name_type)
                if similarity > best_similarity:
                    best_t_name = t_name
                    best_c_name = c_name
                    best_f_name = f_name
                    best_similarity = similarity
        if best_t_name is None or best_c_name is None or best_f_name is None:
            break

        method = methods_table[best_t_name]
        set_default_csv_operation_chain(method=method, column=method.table.columns[best_c_name], 
                            f_name=best_f_name, field_name_type=field_name_type)
        non_assigned_fields.remove(best_f_name)
        assigned_table_opt_column_names[best_t_name].add(best_c_name)

    for t_name, method in sorted(list(methods_table.items()), key=lambda e: e[0]):
        print(f"\nConfiguring columns for table {t_name}:")
        print("")

        def format_column_state(c_name: str):
            profile = method.table_profile
            column = method.table.columns[c_name]
            return f"{c_name}: {profile[column] if column in profile else 'not set'}"


        while True:
            # check required columns are set
            for c_name in table_req_column_names[t_name]:
                req_column = method.table.columns[c_name]
                while req_column not in method.table_profile:
                    print(f"Could not auto-detect field for required column {t_name}.{req_column.name} ")
                    change_column_operations(t_name=t_name, column=req_column, required=True, 
                                field_name_type=field_name_type, profile=method.table_profile)
            req_c_names = list(sorted(table_req_column_names[t_name]))
            opt_c_names = list(sorted(table_opt_colum_names[t_name]))
            if len(req_c_names) + len(opt_c_names) > 0:
                max_c_name_size = max(len(c_name) + len(str(i)) for (i, c_name) in enumerate(req_c_names + opt_c_names))
                
                index = 0
                print(f"Table {t_name}: ")
                if len(req_c_names) > 0:
                    print(f"   - Required columns:")
                    for c_name in req_c_names:
                        print(f"       {index}  {(max_c_name_size-len(c_name)-len(str(index)))*' '}{format_column_state(c_name)}")
                        index += 1
                else:
                    print(f"   - No Required column")
                print("")


                if len(opt_c_names) > 0:
                    print(f"   - Optional columns:")
                    for c_name in opt_c_names:
                        print(f"       {index}  {(max_c_name_size-len(c_name)-len(str(index)))*' '}{format_column_state(c_name)}")
                        index += 1
                else:
                    print(f"   - No optional column")
                print("")

                
            exit_choice = "Everything is fine"
            choosed_c_name = choose(items=req_c_names + opt_c_names, prompt="Choose column to change", 
                        default=exit_choice)
            print(get_suffix(), end="")
            if choosed_c_name == exit_choice:
                break
            else:
                change_column_operations(t_name=t_name, column=method.table.columns[choosed_c_name], 
                            field_name_type=field_name_type, profile=method.table_profile, 
                            required=choosed_c_name in table_req_column_names[t_name])


def change_column_operations(t_name: str, column: SqlColumn, field_name_type: dict[str, DeducedType], 
            profile: TableProfile, required: bool = False):
    c_type = column.make_type()
    title = f"operation chain for {t_name}.{column.name} ({c_type.gen_type()})" + (" [required]" if required else "")
    if column not in profile:
        profile[column] = OperationChain(operations=[])
    op_chain = profile[column]
    print(f"Change {title}:")
    if len(op_chain) > 0:
        print(f"(currently is {op_chain})")
        extend_operations_choice = "Extend operation chain"
        reset_operation_choice = "Reset operation chain"
        remove_operation_choice = "Remove operation chain"
        action = this_print_choose(items=[extend_operations_choice, reset_operation_choice] + ([remove_operation_choice] if not required else []), 
                    prompt="Choose what to do", default=extend_operations_choice)
        if action == reset_operation_choice:
            op_chain.clear()
        elif action == remove_operation_choice:
            del profile[column]
            return
    else:
        print(f"(currently no chain)")
        new_operations_choice = "New operation chain"
        cancel_operation_choice = "Cancel operation chain"
        action = this_print_choose(items=[new_operations_choice] + ([cancel_operation_choice] if not required else []), 
                    prompt="Choose what to do", default=new_operations_choice)
        if action == cancel_operation_choice:
            del profile[column]
            return

    if len(op_chain) == 0:
        use_csv_data_choice = "Use CSV data (column name required)"
        no_csv_data_choice = "No CSV data (random or constant value)"
        items = [use_csv_data_choice, no_csv_data_choice]
        choice = this_print_choose(items=items,
                    prompt="Choose option", default=use_csv_data_choice)
        if choice == use_csv_data_choice:
            f_name = this_print_choose(items=list(field_name_type), prompt="Choose field name", 
                        printer=lambda f_name: f_name + \
                        (f"({t.gen_type()})" if (t:= field_name_type[f_name].get_subtype_assignable_to(c_type)) is not None else ""))
            op_chain.append(make_csv_column_instance(f_name, field_name_type))
        elif choice == no_csv_data_choice:
            op_chain.append(CSV_NO_COLUMN_OP.make_instance())
        
    while True:
        print(f"{title}:")
        print(f"   - {op_chain} ({op_chain.last_type().gen_type()})")

        avaiable_ops = list(o for o in CSV_OPERATIONS.values() 
                    if o.get_input_type().is_super_type(op_chain.last_type()) 
                    and (not op_chain.last_type().is_none() or not o.get_input_type().is_optional()))
        if len(avaiable_ops) == 0:
            print("Error: no available operations")
            return
        default = None
        no_operation_label = "No operation"
        const_enum_operation_label = "Constant enum"
        quit_operation_label = "Quit"
        if (c_type.is_super_type(op_chain.last_type())):
            default = Operation(name=no_operation_label, impl=lambda a,b:None, input_type=PythonType(), 
                        output_type=PythonType())
        if op_chain.last_type().is_none() and c_type.is_enum():
            enum_operation = Operation(name=const_enum_operation_label, impl=lambda a,b:None, 
                        input_type=PythonType(), output_type=PythonType(), 
                        dynamic_output_type_cb=lambda _: BaseEnumPythonType(StrPythonType()))
            avaiable_ops.append(enum_operation)
            if default is None:
                default = enum_operation
        if default is None:
            default = Operation(name=quit_operation_label, impl=lambda a,b:None, input_type=PythonType(), 
                        output_type=PythonType())
        
        next_op = this_gen_print_choose(items=avaiable_ops, prompt="Choose next operation", default=default,
                    printer=lambda o: f"{o.name}{f' (-> {(op_chain.get_next_dynamic_type(o.make_instance([])) if len(o.args) == 0 else None) or o.get_output_type()})' if type(o.get_input_type()) != PythonType else ''}")
        if next_op.name == quit_operation_label:
            del profile[column]
            return
        if next_op.name == no_operation_label:
            return
        elif next_op.name == const_enum_operation_label:
            if not c_type.is_enum():
                raise Exception("Internal error proposed enum completion for column without enum type")
            values = cast(list[str], c_type.enum_values())
            if not all(isinstance(v, str) for v in values):
                raise Exception("Column enum is not of type str")
            enum_value = this_print_choose(items=values, prompt="Choose value for constant")
            arg_values = [enum_value]
            next_op = CONSTANT_STR_OP
        elif next_op.name == ENUM_CONVERTER_OP.name:
            raise NotImplementedError()
        else:
            arg_values: list[object] = list()
            for arg in next_op.args:
                arg_values.append(arg.type.litteral_from_str(this_free_choose(
                            prompt=f"Value for {next_op.name}.{arg.name} ({arg.type.gen_type()})", 
                            invalid_message=f"Invalid choice for type {arg.type.gen_type()}",
                            validator=lambda s: arg.type.is_valid_str_litteral(s))))
        op_chain.append(next_op.make_instance(arg_values))



def create_constant_profile(methods: list[Constant]):
    for method in methods:
        for c_name in method.table.get_primary_column_names():
            column = method.table.columns[c_name]
            if not column.make_type().is_text():
                raise Exception(f"Unique key of type {column.make_type()} (!= str) is not supported")
            pk_value = this_free_choose(prompt=f"Choose value for constant entry {method.table.name}.{c_name}")
            method.table_profile[column] = OperationChain([CONSTANT_STR_OP.make_instance([pk_value])])

def create_profile(tables: dict[str, SqlTable], table_method_names: dict[str, str], 
            field_name_type: dict[str, DeducedType]):
    def explorer(t_name: str):
        for column in tables[t_name].columns.values():
            data_type = column.data_type
            if isinstance(data_type, SqlReferenceType) and \
                        (not column.optional or data_type.table.name in table_method_names):
                yield data_type.table.name
    graph = Graph(nodes=list(tables), explorer=explorer) # type: ignore
    methods = list(METHODS_NAME[table_method_names[t_name]](tables[t_name], TableProfile({})) #type: ignore
            for t_name in graph.sink_to_source_exploration() if t_name in table_method_names) # type: ignore

    csv_methods: list[NormalCsv] = list()
    constant_methods: list[Constant] = list()
    for method in methods:
        if isinstance(method, NormalCsv):
            csv_methods.append(method)
        else:
            constant_methods.append(method)

    create_constant_profile(methods=constant_methods)
    create_normal_csv_profile(methods=csv_methods, field_name_type=field_name_type)
    return CsvProfile(methods)

def get_csv_filepath():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <path_to_csv>")
        exit()
    return sys.argv[1]

class DeducedType:
    def __init__(self, types: Iterable[PythonType], optional: bool = False,
                 values: Optional[set[str]] = None) -> None:
        self.__types = set(types)
        self.optional = optional
        self.values: Optional[set[str]] = set() if values is None else values

    @staticmethod
    def default():
        return DeducedType(types=AUTO_DETECTED_TYPES)

    def infer_from_value(self, value: str):
        if nullify_str(value) is None:
            self.optional = True
            return
        for type in list(self.__types):
            if not type.is_valid_str_litteral(value):
                self.__types.remove(type)
        if self.values is not None:
            self.values.add(upper_str(to_ascii(value)))
            if len(self.values) > ENUM_AUTO_DETECT_LIMIT:
                self.values = None

    def get_subtype_assignable_to(self, type: PythonType):
        for t in self.get_types():
            if type.is_super_type(t):
                return t
        return None
    
    def is_assignable_to(self, type: PythonType):
        return self.get_subtype_assignable_to(type) is not None

    def get_types(self):
        types = list(self.__types)
        if self.values is not None:
            types.append(EnumPythonType(StrPythonType(), list(self.values)))
        if self.optional:
            types = list(OptionalPythonType(t) if t != StrPythonType() else t for t in types)
        return types

    def get_best_type(self):
        def type_sort(type: PythonType):
            if type.is_super_type(FloatPythonType()):
                return 9
            if type.is_super_type(IntPythonType()):
                return 10
            if type.is_super_type(DateTimePythonType()):
                return 7
            if type.is_super_type(TimePythonType()):
                return 8
            if type.is_enum():
                return 1
            return 0
        return max(self.get_types(), key=type_sort)

    def __str__(self):
        return " | ".join(t.gen_type(imports=PythonScript().imports) for t in self.get_types())


def get_field_name_type(csv_filepath: str) -> dict[str, DeducedType]:
    with open(file=csv_filepath, mode="r") as csv_file:
        csv_reader = DictReader(csv_file, delimiter=find_csv_delimiter(csv_filepath))
        if csv_reader.fieldnames is None:
            raise Exception("Invalid File : no header")
        field_name_type: dict[str, DeducedType] = \
                        {f_name: DeducedType.default() for f_name in csv_reader.fieldnames}
        if len(csv_reader.fieldnames) <= 1:
            if not yes_no_choose(prompt="CSV file contains only one column '" + \
                            csv_reader.fieldnames[0] + "', do you want to continue ?"):
                    raise Exception("Invalid File: (maybe) wrong separator")
        for (i, row) in enumerate(csv_reader):
            if None in row or any(v is None for v in row.values()):
                Exception(f"Invalid File : at line {i}, wrong number of values")
            for (k, v) in row.items():
                if k is None:
                    print(row)
                field_name_type[k].infer_from_value(v)
        return field_name_type


def choose_csv_fields(field_name_type: dict[str, DeducedType]):
    max_f_name_size = max(len(f_name) for f_name in field_name_type)
    for f_name, f_type in field_name_type.items():
        print(f" - {(max_f_name_size-len(f_name)) * ' '}{f_name} : {f_type.get_best_type().gen_type()}")
    if not this_yes_no_choose(prompt="Keep all CSV columns ?", default=True):
        while True:
            quit_choice = "Keep those columns"
            choice = this_print_choose(list(field_name_type.keys()), 
                        prompt="Choose column to remove", default=quit_choice)
            if choice == quit_choice:
                break
            del field_name_type[choice]


def generate_profile(database: Database, csv_filepath: str):
    field_name_type = get_field_name_type(csv_filepath=csv_filepath)
    choose_csv_fields(field_name_type)
    tables = generate_schema(database=database)
    table_method_type_names = create_table_method_names(tables=tables, field_name_type=field_name_type)
    profile = create_profile(tables=tables, table_method_names=table_method_type_names, 
                field_name_type=field_name_type)

    return profile


if __name__ == "__main__":
    generate_profile(database=choose_database(), csv_filepath=get_csv_filepath()).to_json_file(filepath="gen/profile.json")

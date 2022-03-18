from __future__ import annotations

from typing import Iterable
from code_gen_config import Config
from csv_addon.importer_code_gen_config import ImportConfig
from operations import BASE_OPERATIONS, Operation, OperationChain
from python_script import BaseEnumPythonType, DictPythonType, NonePythonType, PythonScript, PythonType, PythonVariable, StrPythonType
from sql_objects import SqlColumn, SqlTable
from json import dump, load
from itertools import chain


CSV_COLUMN_COLUMN_NAME = PythonVariable(name="column_name", type=StrPythonType())
def csv_column_impl(script: PythonScript, input: PythonVariable):
    script.add_line(f"return {input}[{CSV_COLUMN_COLUMN_NAME}]")

def csv_no_column_impl(script: PythonScript, input: PythonVariable):
    script.add_line(f"return None")

CSV_COLUMN_OP = Operation(
            name="csv_column", 
            impl=csv_column_impl,
            input_type=DictPythonType(StrPythonType(), StrPythonType()), 
            output_type=StrPythonType(),
            args=[CSV_COLUMN_COLUMN_NAME])

CSV_ENUM_COLUMN_OP = Operation(
            name="csv_enum_column", 
            impl=CSV_COLUMN_OP.impl,
            input_type=CSV_COLUMN_OP.get_input_type(), 
            output_type=BaseEnumPythonType(StrPythonType()),
            args=CSV_COLUMN_OP.args)

CSV_NO_COLUMN_OP = Operation(
            name="csv_no_column", 
            impl=csv_no_column_impl,
            input_type=CSV_COLUMN_OP.get_input_type(),
            output_type=NonePythonType())


CSV_OPERATIONS = {
    op.name: op
    for op in chain((
        CSV_COLUMN_OP,
        CSV_ENUM_COLUMN_OP,
        CSV_NO_COLUMN_OP, 
    ), BASE_OPERATIONS.values())
}





class TableGenerationMethod:
    def __init__(self, table: SqlTable, table_profile: TableProfile, optional: bool = False):
        self.table = table
        self.table_profile = table_profile

    def check_validity(self) -> None:
        raise Exception("Abstract method")

    @staticmethod
    def name() -> str:
        raise Exception("Abstract method")

    @staticmethod
    def is_recursive() -> bool:
        raise Exception("Abstract method")

    @staticmethod
    def write_to_database() -> bool:
        raise Exception("Abstract method")

    def required_columns(self) -> Iterable[SqlColumn]:
        raise Exception("Abstract method")

    def optional_columns(self)-> Iterable[SqlColumn]:
        raise Exception("Abstract method")

    def gen_row_converter_decl(self, script: PythonScript, column_references: dict[str, PythonVariable], 
                    entries_param: PythonVariable, row_param: PythonVariable) -> None:
        raise Exception("Abstract method")

    def gen_row_converter_call(self, script: PythonScript, column_references: dict[str, PythonVariable], 
                    entries_param: PythonVariable, row_param: PythonVariable, prefix: str) -> None:
        raise Exception("Abstract method")
        


class NormalCsv(TableGenerationMethod):
    def __init__(self, table: SqlTable, table_profile: TableProfile, optional: bool = False):
        super().__init__(table, table_profile, optional)

    def check_validity(self) -> None:
        input_type = DictPythonType(key_type=StrPythonType(), value_type=StrPythonType())
        self.table_profile.check_validity(input_type=input_type, 
                required_column_names=map(lambda c: c.name, self.required_columns()))

    @staticmethod
    def name() -> str:
        return "normal_csv"
    
    @staticmethod
    def is_recursive() -> bool:
        return True

    @staticmethod
    def write_to_database() -> bool:
        return True

    def required_columns(self):
        return (c for c in self.table.columns.values() if not c.optional and not c.data_type.is_referrence() and c.default is None)

    def optional_columns(self) -> Iterable[SqlColumn]:
        return (c for c in self.table.columns.values() if (c.optional or c.default is not None) and not c.data_type.is_referrence())

    def gen_row_converter_decl(self, script: PythonScript, 
                               column_references: dict[str, PythonVariable], 
                               entries_param: PythonVariable, row_param: PythonVariable):
        with script.gen_function_decl(
                        function_name=ImportConfig.FunctionName.row_converter(self.table.name), 
                        params=[row_param, entries_param] + list(column_references.values())):
            ref_values = list(f"{c.name}={column_references[c.data_type.get_reffered_table_name()]}" 
                        for c in self.table.columns.values() if c.data_type.is_referrence() 
                        and c.data_type.get_reffered_table_name() in column_references)
            values = list(str(c.name) + "=" + op_chain.gen_operations(imports=script.imports, 
                        gen_input=row_param.name) 
                        for (c, op_chain) in self.table_profile.items()) + ref_values
            start = f"return {entries_param}.{Config.MethodName.Entries.entry_maker(self.table.name)}("
            script.add_aligned_line(separator=",", values=values, start=start, end=")")

    def gen_row_converter_call(self, script: PythonScript, 
                               column_references: dict[str, PythonVariable], 
                               entries_param: PythonVariable, row_param: PythonVariable, prefix: str) -> None:
        values = list(f"{param}={ImportConfig.VariableName.table_object(t_name)}" for (t_name, param) in column_references.items())
        script.add_aligned_line(separator=",", values=values, 
                                start=prefix + ImportConfig.FunctionName.row_converter(self.table.name) + 
                                            f"({row_param}, {entries_param}, ",
                                end=")")
        

class Constant(TableGenerationMethod):
    def __init__(self, table: SqlTable, table_profile: TableProfile, optional: bool = False):
        super().__init__(table, table_profile, optional)

    def check_validity(self) -> None:
        input_type = NonePythonType()
        self.table_profile.check_validity(input_type=input_type,
                required_column_names=self.table.get_primary_column_names())

    @staticmethod
    def name():
        return "constant"

    @staticmethod
    def is_recursive() -> bool:
        return False

    @staticmethod
    def write_to_database() -> bool:
        return False

    def required_columns(self):
        return (self.table.columns[c_name] for c_name in self.table.get_primary_column_names())

    def optional_columns(self) -> Iterable[SqlColumn]:
        return []

    def gen_row_converter_decl(self, script: PythonScript, 
                column_references: dict[str, PythonVariable], entries_param: PythonVariable, 
                row_param: PythonVariable) -> None:
        t_name = self.table.name
        with script.gen_function_decl(
                    function_name=ImportConfig.FunctionName.row_converter(table_name=t_name),
                    params=list()):
            script.add_line(f"return {Config.ClassName.formater(t_name)}.{Config.MethodName.Table.constant}" + "(" +
                ', '.join(f'{pk_name}=' + self.table_profile[self.table.columns[pk_name]].gen_operations(script.imports, repr(None)) 
                for pk_name in self.table.get_primary_column_names()) + ")")
                    

    def gen_row_converter_call(self, script: PythonScript, column_references: dict[str, PythonVariable], 
                entries_param: PythonVariable, row_param: PythonVariable, prefix: str) -> None:
        t_name = self.table.name
        script.add_line(f"{prefix}{ImportConfig.FunctionName.row_converter(table_name=t_name)}()")


METHODS_NAME = {
    m.name():m for m in (NormalCsv, Constant)
}

class TableProfile(dict[SqlColumn, OperationChain]):
    def __init__(self, data: dict[SqlColumn, OperationChain]):
        super().__init__(data)

    def check_validity(self, input_type: PythonType, required_column_names: Iterable[str]):
        c_names = set(c.name for c in self)
        required_column_names = list(required_column_names)
        for rc_name in required_column_names:
            if rc_name not in c_names:
                print(required_column_names)
                raise Exception(f"No column {rc_name} in table profile")
        for (column, operations) in self.items():
            if not operations.is_valid(input_type=input_type, output_type=column.make_type()):
                raise Exception(f"Invalid chain for column {column} in profile")

    @staticmethod
    def from_json_file(filepath: str):
        pass

T_NAME = "table"
M_NAME = "method"
C_NAMES = "columns"
OP_NAME = "name"
OP_PARAMS = "params"

class CsvProfile(list[TableGenerationMethod]):
    def __init__(self, methods: Iterable[TableGenerationMethod]):
        super().__init__(methods)

    def check_validity(self):
        for m in self:
            m.check_validity()

    def gen_operations(self, script: PythonScript):
        already_gen: set[str] = set()
        for m in self:
            for op_chain in m.table_profile.values():
                for op in op_chain:
                    if op.name not in already_gen:
                        op.gen_operation_definition(script)
                        already_gen.add(op.name)


    @staticmethod
    def from_json_file(filepath: str, tables: dict[str, SqlTable]):
        with open(file=filepath, mode="r") as f:
            profile = CsvProfile(list(
                METHODS_NAME[m[M_NAME]](
                    table=tables[m[T_NAME]], 
                    table_profile=TableProfile({
                        tables[m[T_NAME]].columns[c_name]:OperationChain([
                            CSV_OPERATIONS[op[OP_NAME]].make_instance(op[OP_PARAMS])
                            for op in c
                        ])
                        for c_name, c in m[C_NAMES].items()
                    })
                ) 
            for m in load(f)))
        # profile.check_validity()
        return profile



    def to_json_file(self, filepath: str):
        # self.check_validity()
        with open(file=filepath, mode="w") as f:
            dump([{T_NAME: m.table.name, M_NAME: m.name(), C_NAMES:{
                c.name: [{OP_NAME: op.name, OP_PARAMS: list(op.arg_values)} for op in op_chain] 
                for (c, op_chain) in m.table_profile.items()
            }} for m in self], f)
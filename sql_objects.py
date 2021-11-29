from __future__ import annotations
from typing import Any, Iterable, Optional, cast
from python_script import *
from util.util import btos, extract_delimiter
from datetime import datetime
from code_gen_config import Config


class SqlTable:

    PARENT_CLASS = ClassPythonType(class_name=Config.ClassName.parent)

    ATD_OBJECT_PARAM = PythonVariable(name=Config.ParamName.AddToDatabase.objects, 
                                      type=IterablePythonType(AnyPythonType()))

    def __init__(self, name: str):
        self.name = name
        self.columns: dict[str, SqlColumn] = dict()
        self.unique_constraints: list[SqlUniqueConstraint] = list()
        self.primary_constraint: Optional[SqlPrimaryConstraint] = None
        self.class_type = TableClassPythonType(table=self)

    def __repr__(self):
        return f"SqlTable({self.name}, columns: {list(self.columns.values())})"

    def add_column(self, column: SqlColumn):
        if column.name in self.columns:
            raise Exception(f"Column {column} already added in {self}")
        self.columns[column.name] = column
    
    def get_column(self, column_name: str):
        if column_name not in self.columns:
            raise Exception(f"No column {column_name} in {self}")
        return self.columns[column_name]

    def get_columns(self):
        return self.columns.values()

    def remove_column(self, column_name: str):
        self.get_column(column_name=column_name)
        del self.columns[column_name]

    def add_unique_constraint(self, unique_constraint: SqlUniqueConstraint):
        self.unique_constraints.append(unique_constraint)

    def is_unique(self, column_names: list[str]):
        return any(all(c_name in c.column_names for c_name in column_names) 
                   for c in self.unique_constraints)

    def add_primary_constraint(self, primary_constraint: SqlPrimaryConstraint):
        if self.primary_constraint is not None:
            raise Exception(f"{self.name} already has a primary constraint ({self.primary_constraint}), cannot add another one ({primary_constraint})")
        self.primary_constraint = primary_constraint

    def get_primary_column_names(self):
        if self.primary_constraint is None:
            raise Exception(f"No primary key in {self.name}")
        return self.primary_constraint.column_names
    
    def is_primary(self, column_names: list[str]):
        if self.primary_constraint is None:
            return False
        return self.primary_constraint.column_names == column_names

    def is_in_primary(self, column_name: str):
        if self.primary_constraint is None:
            return False
        return column_name in self.primary_constraint.column_names

    def add_foreign_constraint(self, foreign_constraint: SqlForeignConstraint, referred_table: SqlTable):
        assert(referred_table.name == foreign_constraint.referenced_table_name)
        for (column_name, referred_column_name) in zip(foreign_constraint.column_names, 
                                                       foreign_constraint.referenced_column_names):
            column = self.get_column(column_name=column_name)
            if column.data_type.is_referrence():
                continue
            if column.default is not None:
                raise Exception("Foreign constraint with default value")
            refered_column = referred_table.get_column(column_name=referred_column_name)
            referred_is_primary = referred_table.is_primary(column_names=[referred_column_name])
            referred_is_referrence = refered_column.data_type.is_referrence()
            if referred_is_primary or referred_is_referrence:
                if not isinstance(column.data_type, SqlStringType):
                    raise Exception("Reference to primary key other than String not supported")
                if referred_is_referrence:
                    referred_table = cast(SqlReferenceType, refered_column.data_type).table
                column.data_type = SqlReferenceType(table=referred_table) 
                return True
        return False

    def pretty_print(self):
        print((len(self.name) + 8) * "#")
        print(f"##  {self.name}  ##")
        print((len(self.name) + 8) * "#")
        for (column_name, column) in self.columns.items():
            print("   ", end="")
            if self.primary_constraint is not None and column_name in self.primary_constraint.column_names:
                print("[PK]", end="")
            else:
                print("    ", end="")
            print(f" - {column}")
        print("")
        for unique_constraint in self.unique_constraints:
            print(f"unique({', '.join(unique_constraint.column_names)})")

    def get_sorted_columns(self):
        def column_order(column: SqlColumn):
            if self.is_in_primary(column_name=column.name):
                return 0
            if column.default:
                if column.optional:
                    return 3
                return 4
            if column.optional:
                return 2
            return 1
        return sorted(self.columns.values(), key=column_order)

    def get_all_unique_constraints(self):
        constraints = self.unique_constraints
        if self.primary_constraint is not None and Config.Semantic.use_primary_key_in_equality:
            constraints.append(self.primary_constraint)
        return constraints

    def get_common_columns_in_constraints(self, constraints: Iterable[SqlUniqueConstraint]):
        common_column_names_in_constraints = set(self.columns.keys())
        all_column_names = set(self.columns.keys())
        for constraint in constraints:
            common_column_names_in_constraints.difference_update(
                    all_column_names.difference(constraint.column_names))
        return common_column_names_in_constraints

    def field_variables(self):
        return (c.make_variable() for c in self.get_sorted_columns())


    def gen_ctx(self, parent_script: PythonScript):
        with parent_script.gen_method_decl(method_name="__init__", params=self.field_variables()) as script:
            for field in self.field_variables():
                script.add_line(f"self.{field.name} = {field.name}")


    def gen_eq(self, parent_script: PythonScript):
        var_other = PythonVariable(name="o", type=AnyPythonType())
        with parent_script.gen_method_decl(method_name="__eq__", params=[var_other]) as eq_script:
            if len(self.get_all_unique_constraints()) == 0:
                eq_script.add_line("return False")
            else:
                table_name = self.class_type.gen_type(imports=eq_script.imports)
                with eq_script.gen_if(f"not isinstance({var_other.name}, {table_name})") \
                        as if_not_inst_script:
                    if_not_inst_script.add_line("return False")
                eq_script.add_aligned_line(start="return ", end="", separator=" or ", 
                        values=(" and ".join(f"self.{c_name} == o.{c_name}" for c_name in c.column_names) 
                                for c in self.get_all_unique_constraints()), 
                        newline_after_separator=False, escape_after_newline=True)

    def gen_hash(self, parent_script: PythonScript):
        common_column_names_in_constraints = \
                    self.get_common_columns_in_constraints(self.get_all_unique_constraints())
        with parent_script.gen_method_decl(method_name="__hash__") as script:
            if len(common_column_names_in_constraints) > 0:
                script.add_aligned_line(start = "return ", end="", separator=" + ", 
                            values=(f"hash(self.{c})" for c in common_column_names_in_constraints),
                            newline_after_separator=False, escape_after_newline=True)
            else:
                script.add_line(f"return {Config.Constant.default_hash}")

    def gen_atd(self, parent_script: PythonScript):
        objects_param = SqlTable.ATD_OBJECT_PARAM
        cast(IterablePythonType, objects_param.type).inner_type = self.class_type
        connection_param = SqlTable.get_connection_param(imports=parent_script.imports)
        with SqlTable.gen_atd_decl(parent_script=parent_script, objects_param=objects_param, 
                    connection_param=connection_param) as script:
            statement_var = PythonVariable(name=Config.VariableName.AddToDatabase.statement)
            stmt_marker = "%s"
            start_assign = f"{statement_var} = "
            start = start_assign + f'"INSERT INTO {self.name} (" '
            end_indent = len(start_assign) * ' '
            indented_end = f"{end_indent}\"VALUES ({', '.join(stmt_marker for _ in self.columns)})\""
            values = list('"' + c_name for c_name in self.columns)
            script.add_aligned_line(separator=', "', values=values, start=start, end=') "', 
                        indented_end=indented_end, escape_after_newline=True)

            values_var = PythonVariable(name=Config.VariableName.AddToDatabase.values)
            object_var = PythonVariable(name=Config.VariableName.AddToDatabase.object)
            script.add_aligned_line(start=f"{values_var} = list([", separator=",", 
                    values=(c.gen_get_sql_value(column_var_name=f"{object_var}.{c.name}") 
                        for c in self.columns.values()),
                    end=f"] for {object_var} in {objects_param})")
            with script.gen_if(f"len({values_var}) == 0") as case_empty_script:
                case_empty_script.add_line("return")
            cursor_var = PythonVariable(name=Config.VariableName.AddToDatabase.cursor)
            script.add_line(f"{cursor_var} = {connection_param}" \
                        f".{Config.SqlConnector.Methods.connection_new_cursor}()")
            script.add_line(f"{cursor_var}.{Config.SqlConnector.Methods.cursor_execute_many}" \
                        f"({statement_var}, {values_var})")
            script.add_line(f"{cursor_var}.{Config.SqlConnector.Methods.cursor_close}()")

    def gen_constant(self, parent_script: PythonScript):
        if self.primary_constraint is None:
            return
        pk_vars: list[PythonVariable] = list()
        non_pk_vars: list[PythonVariable] = list()
        for f in self.field_variables():
            if f.name in self.primary_constraint.column_names:
                pk_vars.append(f)
            else:
                non_pk_vars.append(f)
        with parent_script.gen_static_method_decl(method_name=Config.MethodName.Table.constant,
                    params=pk_vars) as script:
            values = list(f"{f}={f}" for f in pk_vars)
            for field in non_pk_vars:
                field_type = cast(PythonType, field.type)
                values.append(f"{field}={field_type.gen_litteral(field_type.default_literal(), imports=script.imports)}")
            script.add_aligned_line(start=f"return {self.class_type.gen_type(imports=script.imports)}(",
                        end=")", separator=",", values=values)

    def generate(self, script: PythonScript):
        with script.gen_class_decl(class_type=self.class_type) as script:
            self.gen_ctx(parent_script=script)
            self.gen_atd(parent_script=script)
            self.gen_constant(parent_script=script)
            self.gen_eq(parent_script=script)
            self.gen_hash(parent_script=script)

    @staticmethod
    def get_connection_param(imports: PythonImports):
        imports.add_import_from_config(Config.SqlConnector.Imports.connection_type)
        return PythonVariable(name=Config.ParamName.connection, 
                    type=ClassPythonType(Config.SqlConnector.Imports.connection_type.name()))
        
    @staticmethod
    def gen_atd_decl(parent_script: PythonScript, 
                objects_param: PythonVariable, connection_param: PythonVariable):
        return parent_script.gen_static_method_decl(method_name=Config.MethodName.Table.add_to_database, 
                    params=[objects_param, connection_param], return_type=NonePythonType())
        

    @staticmethod
    def gen_parent_method(parent_script: PythonScript):
        with parent_script.gen_class_decl(class_type=SqlTable.PARENT_CLASS) as script:
            with SqlTable.gen_atd_decl(parent_script=script, 
                        objects_param=SqlTable.ATD_OBJECT_PARAM, 
                        connection_param=SqlTable.get_connection_param(imports=script.imports)) \
                         as atd_script:
                atd_script.add_line(f'raise Exception("{Config.Message.abstract_method_error}")')

class TableClassPythonType(BaseClassPythonType):
    def __init__(self, table: SqlTable):
        super().__init__(class_name=Config.ClassName.formater(table.name), 
                    parent=SqlTable.PARENT_CLASS)
        self.table = table

    def gen_litteral(self, value: dict[str, Any], imports: PythonImports) -> str:
        return f"{self.gen_type(imports=imports)}.{Config.MethodName.Table.constant}(" + \
            ", ".join(f"{c_name}={self.table.get_column(c_name).make_type().gen_litteral(v, imports=imports)}" \
            for (c_name, v) in value.items()) + ")"

    def default_literal(self) -> dict[str, Any]:
        if self.table.primary_constraint is None:
            raise Exception("Cannot generate default literral for a table without primary key")
        return {f_name: self.table.get_column(f_name).make_type().default_literal() 
                    for f_name in self.table.primary_constraint.column_names}

class SqlColumn:
    def __init__(self, name: str, data_type: SqlType, optional: bool, default: Optional[bytes]):
        self.name = name
        self.optional = optional
        self.data_type = data_type
        self.default = default

    def make_type(self):
        python_type = self.data_type.python_type
        if self.optional:
            python_type = OptionalPythonType(python_type)
        return python_type

    def make_variable(self):
        initial_value = None
        if self.default:
            initial_value = self.data_type.from_bytes(self.default)
        return PythonVariable(name=self.name, type=self.make_type(), initial_litteral=initial_value,
                initial_value_if_optional=Config.Semantic.default_value_for_optional_column)
        
    def __repr__(self):
        return f"SqlColumn({self.name}, {self.data_type}{', optional' if self.optional else ''}" \
                         f"{f', default({self.data_type.from_bytes(self.default)})' if self.default else ''})"
                
    def gen_get_sql_value(self, column_var_name: str):
        type_conversion = self.data_type.gen_get_sql_value()
        return column_var_name + type_conversion + (f" if {column_var_name} is not None else None" \
                    if len(type_conversion) > 0 and self.optional else "")


class SqlType:
    def __init__(self):
        self.python_type = PythonType()
        
    def __repr__(self) -> str:
        raise Exception("abstract method")

    def from_bytes(self, data: bytes) -> Any:
        raise Exception("abstrac method")

    def is_referrence(self) -> bool:
        return False

    def gen_get_sql_value(self) -> str:
        return ""


class SqlIntegerType(SqlType):
    def __init__(self, precision: int):
        super().__init__()
        self.precision = precision
        self.python_type = IntPythonType()

    def __repr__(self) -> str:
        return f"SqlInteger({self.precision})"

    def from_bytes(self, data: bytes) -> int:
        return int(btos(data))
    

class SqlFloatType(SqlType):
    def __init__(self, precision: int):
        super().__init__()
        self.precision = precision
        self.python_type = FloatPythonType()

    def __repr__(self) -> str:
        return f"SqlFloat({self.precision})"

    def from_bytes(self, data: bytes) -> float:
        return float(btos(data))


class SqlJsonType(SqlType):
    def __init__(self):
        super().__init__()
        self.python_type = StrPythonType()

    def __repr__(self) -> str:
        return "SqlJson"

    def from_bytes(self, data: bytes) -> str:
        return btos(data)


class SqlStringType(SqlJsonType):
    def __init__(self, char_limit: int):
        super().__init__()
        self.char_limit = char_limit

    def __repr__(self) -> str:
        return f"SqlString({self.char_limit})"

class SqlDateTimeType(SqlType):
    def __init__(self):
        super().__init__()
        self.python_type = DateTimePythonType()

    def __repr__(self) -> str:
        return "SqlDateTime"

    def from_bytes(self, data: bytes) -> datetime:
        return datetime.strptime(btos(data), "%Y-%m-%d %H:%M:%S")


class SqlTimeType(SqlType):
    def __init__(self):
        super().__init__()
        self.python_type = TimePythonType()

    def __repr__(self) -> str:
        return "SqlTime"

    def from_bytes(self, data: bytes) -> datetime:
        raise NotImplementedError()


class SqlBlobType(SqlType):
    def __init__(self, char_limit: int):
        super().__init__()
        self.char_limit = char_limit
        self.python_type = BytesPythonType()

    def __repr__(self) -> str:
        return f"SqlBlob({self.char_limit})"

    def from_bytes(self, data: bytes) -> bytes:
        return data


class SqlEnumType(SqlJsonType):
    def __init__(self, values: list[str]):
        super().__init__()
        self.values = values
        self.python_type = EnumPythonType(values=values)

    def __repr__(self) -> str:
        return f"SqlEnum({', '.join(self.values)})"

    def from_bytes(self, data: bytes) -> str:
        value = btos(data)
        if value not in self.values:
            raise Exception(f"Cannot convert {data} to {self}")
        return value


class SqlReferenceType(SqlType):
    def __init__(self, table: SqlTable):
        super().__init__()
        self.table = table
        self.python_type = TableClassPythonType(self.table)
    
    def __repr__(self) -> str:
        return f"SqlReference({self.table.name})"

    def from_bytes(self, data: bytes) -> Any:
        raise Exception("Cannot convert bytes to reference")

    def is_referrence(self) -> bool:
        return True

    def gen_get_sql_value(self) -> str:
        if self.table.primary_constraint is None:
            raise Exception("Internal Exception : reference to a table without primary key")
        primary_column_names = self.table.primary_constraint.column_names
        if len(primary_column_names) == 0:
            raise Exception("Internal Exception : reference to a table primary key of size 0")
        if len(primary_column_names) > 1:
            raise NotImplementedError()
        return "." + primary_column_names[0]



class SqlUniqueConstraint:
    def __init__(self, column_names: list[str]):
        self.column_names = column_names

    def __repr__(self) -> str:
        return f"SqlUnique(({' | '.join(self.column_names)}))"

    @staticmethod
    def type() -> str:
        return "UNIQUE"

class SqlPrimaryConstraint(SqlUniqueConstraint):
    def __init__(self, column_names: list[str]):
        super().__init__(column_names)

    def __repr__(self) -> str:
        return f"SqlPrimary({extract_delimiter(super().__repr__())})"

    @staticmethod
    def type() -> str:
        return "PRIMARY KEY"

class SqlForeignConstraint(SqlUniqueConstraint):
    def __init__(self, column_names: list[str], referenced_table_name: str, 
                 referenced_column_names: list[str]):
        super().__init__(column_names)
        self.referenced_table_name = referenced_table_name
        self.referenced_column_names = referenced_column_names

    def __repr__(self) -> str:
        return f"SqlForeign({extract_delimiter(super().__repr__())} " \
               f"-> {self.referenced_table_name}.({' | '.join(self.referenced_column_names)})"

    @staticmethod
    def type() -> str:
        return "FOREIGN KEY"

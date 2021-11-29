from __future__ import annotations
from typing import Iterable
from operations import BASE_OPERATIONS, Operation, OperationChain
from python_script import DictPythonType, NonePythonType, PythonType, StrPythonType
from sql_objects import SqlColumn, SqlTable


CSV_COLUMN = Operation(name="csv_column", input_type=DictPythonType(StrPythonType(), StrPythonType()),
            output_type=StrPythonType(), args=[StrPythonType()])

CSV_NO_COLUMN = Operation(name="csv_no_column", input_type=DictPythonType(StrPythonType(), StrPythonType()),
            output_type=NonePythonType())


CSV_OPERATIONS = BASE_OPERATIONS + [
    CSV_COLUMN,
    CSV_NO_COLUMN,
]


class TableGenerationMethod:
    def __init__(self, table: SqlTable, table_profile: TableProfile) -> None:
        self.table = table
        self.table_profile = table_profile

    def check_validity(self) -> None:
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
        


class NormalCsv(TableGenerationMethod):
    def __init__(self, table: SqlTable, table_profile: TableProfile) -> None:
        super().__init__(table, table_profile)

    def check_validity(self) -> None:
        input_type = DictPythonType(key_type=StrPythonType(), value_type=StrPythonType())
        self.table_profile.check_validity(input_type=input_type, 
                required_column_names=(c.name for c in self.table.get_columns() if not c.optional))
    
    @staticmethod
    def is_recursive() -> bool:
        return True

    @staticmethod
    def write_to_database() -> bool:
        return True

    def required_columns(self):
        return (c for c in self.table.columns.values() if not c.optional)

    def optional_columns(self) -> Iterable[SqlColumn]:
        return (c for c in self.table.columns.values() if c.optional)
        

class Constant(TableGenerationMethod):
    def __init__(self, table: SqlTable, table_profile: TableProfile) -> None:
        super().__init__(table, table_profile)

    def check_validity(self) -> None:
        input_type = NonePythonType()
        self.table_profile.check_validity(input_type=input_type,
                required_column_names=self.table.get_primary_column_names())

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


class TableProfile(dict[SqlColumn, OperationChain]):
    def __init__(self, data: dict[SqlColumn, OperationChain]):
        super().__init__(data)

    def check_validity(self, input_type: PythonType, required_column_names: Iterable[str]):
        c_names = set(c.name for c in self)
        for rc_name in required_column_names:
            if rc_name not in c_names:
                raise Exception(f"No column {rc_name} in profile")
        for (column, operations) in self.items():
            if not operations.is_valid(input_type=input_type, output_type=column.make_type()):
                raise Exception(f"Invalid chain for column {column} in profile")

    @staticmethod
    def from_json_file(filepath: str):
        pass

    def generate_importer(self):
        pass


class CsvProfile(list[TableGenerationMethod]):
    def __init__(self, methods: Iterable[TableGenerationMethod]):
        super().__init__(methods)

    @staticmethod
    def from_json_file(filepath: str):
        pass

    def to_json_file(self, filepath: str):
        pass
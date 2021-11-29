from typing import Iterable
from python_script import AnyPythonType, FloatPythonType, IntPythonType, PythonType, StrPythonType
from util.util import pairs


class Operation:
    def __init__(self, name: str, input_type: PythonType, output_type: PythonType, 
                 primary: bool = False, args: list[PythonType] = []):
        self.name = name
        self.input_type = input_type
        self.output_type = output_type
        self.primary = primary
        self.args = args

class OperationChain(list[Operation]):
    def __init__(self, operations: Iterable[Operation]) -> None:
        super().__init__(operations)
    
    def is_valid(self, input_type: PythonType, output_type: PythonType):
        return self[0].input_type.is_super_type(input_type) \
           and output_type.is_super_type(self[-1].output_type) \
           and all(o2.input_type.is_super_type(o1.output_type) for (o1, o2) in pairs(self))


BASE_OPERATIONS = [
    Operation("upper_str", input_type=StrPythonType(), output_type=StrPythonType()),
    Operation("lower_str", input_type=StrPythonType(), output_type=StrPythonType()),

    Operation("parse_int", input_type=IntPythonType(), output_type=StrPythonType()),
    Operation("stringify_int", input_type=StrPythonType(), output_type=IntPythonType()),

    Operation("parse_float", input_type=FloatPythonType(), output_type=StrPythonType()),
    Operation("stringify_float", input_type=StrPythonType(), output_type=FloatPythonType()), 

    Operation("uuid4_generator", input_type=AnyPythonType(), output_type=StrPythonType(), primary=True),
    Operation("hex_color_generator", input_type=AnyPythonType(), output_type=StrPythonType()),
]
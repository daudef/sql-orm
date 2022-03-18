from typing import Callable, Iterable, Optional
from python_script import *
from util.util import pairs



class Operation:
    def __init__(self, name: str,impl: Callable[[PythonScript, PythonVariable], None], 
                    input_type: PythonType, output_type: PythonType, primary: bool = False, 
                    args: list[PythonVariable] = [], 
                    dynamic_output_type_cb: Optional[Callable[[list[object]], PythonType]] = None):
        self.name = name
        self.impl = impl
        self.__input_type = input_type
        self.__output_type = output_type
        self.primary = primary
        self.args = args
        self.dynamic_output_type_cb = dynamic_output_type_cb

    def get_input_type(self):
        return self.__input_type

    def get_output_type(self):
        return self.__output_type

    def make_instance(self, arg_values: list[object] = [], dynamic_output_type: Optional[PythonType] = None):
        if len(arg_values) != len(self.args):
            raise Exception(f"{len(self.args)} arguments are necessary to make a {self.name}, but only {len(arg_values)} are provided")
        for i, (arg_value, arg) in enumerate(zip(arg_values, self.args)):
            if not arg.type.is_valid_litteral(arg_value):
                raise Exception(f"Argument {i} of {self.name}: {arg_value} cannot be converted to {arg.type}")
        if self.dynamic_output_type_cb is not None:
            dynamic_output_type = self.dynamic_output_type_cb(arg_values)
        return OperationInstance(self, arg_values, dynamic_output_type)

    def make_default_instance(self, dynamic_output_type: Optional[PythonType] = None):
        return self.make_instance(list(a.get_default_litteral() for a in self.args))

class OperationInstance(Operation):
    def __init__(self, op: Operation, arg_values: list[object], dynamic_output_type: Optional[PythonType] = None) -> None:
        super().__init__(op.name, op.impl, op.get_input_type(), op.get_output_type(), primary=op.primary, args=op.args)
        self.arg_values = arg_values
        self.dynamic_output_type = dynamic_output_type

    def __str__(self) -> str:
        return self.name + (f"({', '.join(a.type.gen_litteral(v) for (v, a) in zip(self.arg_values, self.args))})" if len(self.arg_values) > 0 else "")

    def get_output_type(self):
        return self.dynamic_output_type or super().get_output_type()

    def execute(self, input: object):
        if not self.get_input_type().is_valid_litteral(input):
            raise Exception(f"Cannot exec {self.name} with argument {input}")
        script = PythonScript()
        param = PythonVariable(name="i", type=self.get_input_type())
        with script.gen_function_decl(function_name="f", params=[param]):
            for (a, v) in zip(self.args, self.arg_values):
                script.add_line(f"{a} = {a.type.gen_litteral(v)}")
            self.impl(script, param)
        exec(script.get_script())
        return cast(object, eval(f"f({param.type.gen_litteral(input)})"))

    def gen_operation_definition(self, script: PythonScript):
        with script.gen_function_decl(function_name=self.name, params=self.args):
            input_var = PythonVariable(
                        name=Config.ParamName.operation_input,type=self.get_input_type())
            with script.gen_function_decl(
                            function_name=Config.FunctionName.inner_operation,
                            params=[input_var]):
                self.impl(script, input_var)
            script.add_line(f"return {Config.FunctionName.inner_operation}")

    def gen_operation(self, gen_input: str):
        op_args = f"({', '.join(a.type.gen_litteral(v) for (v, a) in zip(self.arg_values, self.args))})"
        return f"{self.name}{op_args}({gen_input})"

class OperationChain(list[OperationInstance]):
    def __init__(self, operations: Iterable[OperationInstance]) -> None:
        super().__init__(operations)
    
    def is_valid(self, input_type: PythonType, output_type: PythonType):
        return self[0].get_input_type().is_super_type(input_type) \
           and output_type.is_super_type(self[-1].get_output_type()) \
           and all(o2.get_input_type().is_super_type(o1.get_output_type()) for (o1, o2) in pairs(self))

    def first_type(self):
        return self[0].get_input_type()
    
    def last_type(self):
        return self[-1].get_output_type()

    def __str__(self):
        return " -> ".join(str(o) for o in self)

    def has_next_dynamic_type(self):
        return len(self) > 0 and (self.last_type().is_enum() or self.last_type().is_none())

    def get_next_dynamic_type(self, op: OperationInstance):
        if op.dynamic_output_type is not None:
            return op.dynamic_output_type
        def make_enum_type(values: Iterable[object], type: PythonType):
            new_values: list[object] = list()
            optional = False
            for value in values:
                try:
                    new_value = op.execute(value)
                except Exception:
                    continue
                if new_value is None:
                    optional = True
                else:
                    new_values.append(new_value)
            inner_type = op.get_output_type()
            while (inner_type.is_enum() or inner_type.is_optional()):
                inner_type = inner_type.get_raw_type()
            enum_type = EnumPythonType(inner_type, new_values)
            if optional:
                enum_type = OptionalPythonType(enum_type)
            return enum_type
        try:
            last_op = self.last_type()
            values: list[object] = list()
            if last_op.is_enum():
                values.extend(last_op.enum_values())
            if last_op.is_none() or last_op.is_optional():
                values.append(None)
            if len(values) == 0:
                return None
            return make_enum_type(values, last_op)
        except Exception:
            return None

    def append(self, op: OperationInstance) -> None:
        if len(self) > 0 and op.dynamic_output_type is None and self.has_next_dynamic_type():
            op.dynamic_output_type = self.get_next_dynamic_type(op)
        super().append(op)

    def gen_operations(self, imports: PythonImports, gen_input: str):
        for op in self:
            gen_input = op.gen_operation(gen_input)
        return gen_input


############# Implementation of operations #############

def upper_str_impl(script: PythonScript, input: PythonVariable):
    script.add_line(f"return {input}.upper()")

def lower_str_impl(script: PythonScript, input: PythonVariable):
    script.add_line(f"return {input}.lower()")

def remove_space_impl(script: PythonScript, input: PythonVariable):
    script.add_line(f'return "".join(_c for _c in {input} if not _c.isspace())') 

def nullify_str_impl(script: PythonScript, input: PythonVariable):
    with script.gen_if(f"len({input}.strip()) == 0"):
        script.add_line("return None")
    script.add_line(f"return {input}")

CONSTANT_STR_VALUE = PythonVariable(name="s", type=StrPythonType())
def constant_str_impl(script: PythonScript, input: PythonVariable):
    script.add_line(f"return {CONSTANT_STR_VALUE}")


def to_ascii_impl(script: PythonScript, input: PythonVariable):
    special_char_map = PythonVariable(
            name="SPECIAL_CHAR_MAP",
            type=DictPythonType(IntPythonType(), StrPythonType()),
            initial_litteral={192: "A", 193: "A", 194: "A", 195: "A", 196: "A", 197: "A", 198: "AE",
                              199: "C", 200: "E", 201: "E", 202: "E", 203: "E", 204: "I", 205: "I",
                              206: "I", 207: "I", 208: "D", 209: "N", 210: "O", 211: "O", 212: "O",
                              213: "O", 214: "O", 216: "O", 217: "U", 218: "U", 219: "U", 220: "U",
                              221: "Y", 223: "B", 224: "a", 225: "a", 226: "a", 227: "a", 228: "a",
                              229: "a", 230: "ae", 231: "c", 232: "e", 233: "e", 234: "e", 235: "e",
                              236: "i", 237: "i", 238: "i", 239: "i", 241: "n", 242: "o", 243: "o",
                              244: "o", 245: "o", 246: "o", 249: "u", 250: "u", 251: "u", 252: "u",
                              253: "y", 255: "y"})
    script.globals.add_global(special_char_map)
    char_input = PythonVariable(name="c", type=StrPythonType())
    inner_f_name = "ascii_char"
    with script.gen_function_decl(function_name=inner_f_name, params=[char_input]):
        ascii_num_var = PythonVariable("ascii_num")
        script.add_line(f"{ascii_num_var} = ord({char_input})")
        with script.gen_if(f"{ascii_num_var} < 128"):
            script.add_line(f"return {char_input}")
        with script.gen_try():
            script.add_line(f"return {special_char_map}[{ascii_num_var}]")
        with script.gen_except(exception_typename="KeyError"):
            script.add_line('return " "')
    script.add_line(f'return " ".join("".join({inner_f_name}({char_input}) for {char_input} in {input}).split())')


def uuid4_generator_impl(script: PythonScript, input: PythonVariable):
    script.imports.add_import(module="uuid", object="uuid4")
    script.add_line("return str(uuid4())")

def hex_color_generator_impl(script: PythonScript, input: PythonVariable):
    script.imports.add_import(module="random", object="choice")
    script.add_line('return "#" + "".join(choice("0123456789abcdef") for _ in range(6))')

def parse_int_impl(script: PythonScript, input: PythonVariable):
    script.add_line(f"return int({input})")

def parse_opt_int_impl(script: PythonScript, input: PythonVariable):
    with script.gen_if(f"{input} is None"):
        script.add_line(f"return None")
    script.add_line(f"return int({input})")
    
def parse_float_impl(script: PythonScript, input: PythonVariable):
    script.add_line(f"return float({input})")

def parse_opt_float_impl(script: PythonScript, input: PythonVariable):
    with script.gen_if(f"{input} is None"):
        script.add_line(f"return None")
    script.add_line(f"return float({input})")

def stringify_int_impl(script: PythonScript, input: PythonVariable):
    script.add_line(f"return str({input})")

def stringify_float_impl(script: PythonScript, input: PythonVariable):
    script.add_line(f"return str({input})")

ENUM_CONVERTER_ENUM_ASSO = PythonVariable(name="enum_asso", type=DictPythonType(StrPythonType(), StrPythonType()))
def enum_converter_impl(script: PythonScript, input: PythonVariable):
    script.add_line(f"return {ENUM_CONVERTER_ENUM_ASSO}[{input}]")



# continue transitioning to new implementation (was doing thing with global variables)
# then stop returning new script on new scope

UPPER_STR_OP = Operation("upper_str", upper_str_impl, input_type=StrPythonType(), output_type=StrPythonType())
LOWER_STR_OP = Operation("lower_str", lower_str_impl, input_type=StrPythonType(), output_type=StrPythonType())
REMOVE_SPACE_STR_OP = Operation("remove_space", remove_space_impl, input_type=StrPythonType(), output_type=StrPythonType())
NULLIFY_STR_OP = Operation("nullify_str", nullify_str_impl, input_type=StrPythonType(), output_type=OptionalPythonType(StrPythonType()))
CONSTANT_STR_OP = Operation("constant_str", constant_str_impl, input_type=NonePythonType(), output_type=StrPythonType(), args=[CONSTANT_STR_VALUE], dynamic_output_type_cb=lambda l: EnumPythonType(StrPythonType(), l))
TO_ASCII_OP = Operation("to_ascii", to_ascii_impl, input_type=StrPythonType(), output_type=StrPythonType())
STRINGIFY_FLOAT_OP = Operation("stringify_float", stringify_float_impl, input_type=FloatPythonType(), output_type=StrPythonType())
PARSE_FLOAT_OP = Operation("parse_float", parse_float_impl, input_type=StrPythonType(), output_type=FloatPythonType())
PARSE_OPT_FLOAT_OP = Operation("parse_opt_float", parse_opt_float_impl, input_type=OptionalPythonType(StrPythonType()), output_type=OptionalPythonType(FloatPythonType()))
STRINGIFY_INT_OP = Operation("stringify_int", stringify_int_impl, input_type=IntPythonType(), output_type=StrPythonType())
PARSE_INT_OP = Operation("parse_int", parse_int_impl, input_type=StrPythonType(), output_type=IntPythonType())
PARSE_OPT_INT_OP = Operation("parse_opt_int", parse_opt_int_impl, input_type=OptionalPythonType(StrPythonType()), output_type=OptionalPythonType(IntPythonType()))
UUID4_GENERATOR = Operation("uuid4_generator", uuid4_generator_impl, input_type=NonePythonType(), output_type=StrPythonType(), primary=True)
HEX_COLOR_GENERATOR = Operation("hex_color_generator", hex_color_generator_impl, input_type=NonePythonType(), output_type=StrPythonType())
ENUM_CONVERTER_OP = Operation("enum_converter", enum_converter_impl, input_type=BaseEnumPythonType(StrPythonType()), output_type=BaseEnumPythonType(StrPythonType()), args=[ENUM_CONVERTER_ENUM_ASSO], dynamic_output_type_cb=lambda l: EnumPythonType(StrPythonType(), list(cast(dict[str, str], l[0]).values())))

BASE_OPERATIONS = {
    op.name: op
    for op in (
        UPPER_STR_OP,
        LOWER_STR_OP, 
        REMOVE_SPACE_STR_OP, 
        NULLIFY_STR_OP, 
        CONSTANT_STR_OP, 
        TO_ASCII_OP, 
        STRINGIFY_FLOAT_OP, 
        PARSE_FLOAT_OP, 
        PARSE_OPT_FLOAT_OP,
        STRINGIFY_INT_OP,
        PARSE_INT_OP, 
        PARSE_OPT_INT_OP,
        UUID4_GENERATOR,
        HEX_COLOR_GENERATOR,
        ENUM_CONVERTER_OP,
    )
}
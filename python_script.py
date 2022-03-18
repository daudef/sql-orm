from __future__ import annotations
from collections import defaultdict
from typing import Callable, Iterable, Optional, cast
from code_gen_config import Config, ConfigImport
from datetime import datetime, time
from ast import literal_eval

class PythonScript:

    def __init__(self, parent_script: Optional[PythonScript] = None, on_close: Optional[Callable[[PythonScript], None]] = None):
        self.__script = ""
        self.__current_indent = 0
        self.current_prefix = ""
        self.imports = PythonImports()
        self.imports.add_import(module="__future__", object="annotations")
        self.globals = PythonGlobals()
        self.parent_script = parent_script
        self.on_close = on_close
        self.__on_close_cb: list[Callable[[PythonScript], None]] = list()

    def add_line(self, line: str):
        self.__script += self.current_prefix + line + "\n"
        return self

    def add_aligned_line(self, separator: str, values: Iterable[str], start: str = "",
                    end: str = "", indented_end: str = "", newline_after_separator: bool = True, 
                    escape_after_newline: bool = False, offset: int = 0):
        newline = " \\\n" if escape_after_newline else "\n"
        space = self.current_prefix + (len(start) - 
                    (len(separator) if not newline_after_separator else 0) + offset) * " "
        pretty_separator = separator + newline + space if newline_after_separator else newline + space + separator
        line = start + pretty_separator.join(values) + end
        if len(indented_end) > 0:
            line += newline + self.current_prefix + indented_end
        self.add_line(line)

    def add_indent(self, value: int = 1):
        self.__current_indent += value
        self.__update_current_prefix()

    def remove_indent(self, value: int = 1):
        self.__current_indent -= value
        self.__update_current_prefix()

    def __update_current_prefix(self):
        self.current_prefix = self.__current_indent * Config.Format.indent

    def empty_lines(self, n_empty_lines: int = 1):
        for _ in range(n_empty_lines):
            self.add_line(line="")

    def __add_imports_to_script(self):
        if len(self.imports) > 0:
            dummy_script = PythonScript()
            self.imports.gen(dummy_script)
            self.__script = dummy_script.__script + Config.Format.EmptyLines.after_imports * "\n" \
                            + self.__script

    def __add_globals_to_script(self):
        if len(self.globals) > 0:
            dummy_script = PythonScript()
            self.globals.gen(dummy_script)
            self.__script = dummy_script.__script + Config.Format.EmptyLines.after_globals * "\n" \
                            + self.__script

    def get_script(self):
        self.__add_globals_to_script()
        self.__add_imports_to_script()
        return self.__script

    def add_sub_script(self, other: PythonScript):
        self.imports.update(other.imports)
        self.globals.update(other.globals)
        self.__script += other.__script

    def __enter__(self):
        self.add_indent(1)
        return self

    def __exit__(self, exit: object, value: object, exc: object):
        self.remove_indent(1)
        if self.on_close is not None:
            self.on_close(self)
        if self.parent_script is None:
            raise Exception("Cannot exit script without parent")
        self.parent_script.add_sub_script(self)

    # def __new_scope(self, on_close: Optional[Callable[[PythonScript], None]] = None):
    #     child = PythonScript(parent_script=self, on_close=on_close)
    #     child.add_indent(value=self.__current_indent)
    #     return child

    def gen_class_decl(self, class_type: BaseClassPythonType):
        class_name = class_type.gen_type(self.imports)
        parent_type = class_type.get_parent()
        parent = f"({parent_type.gen_type(self.imports)})" if parent_type is not None else ''
        self.add_line(line=f"class {class_name}{parent}:")
        return PythonScope(self, make_on_close_empty_lines(Config.Format.EmptyLines.after_class))

    def gen_function_decl(self, function_name: str, params: Iterable[PythonVariable] = list(), 
                    return_type: Optional[PythonType] = None):
        end = '):'
        if return_type is not None:
            end = f") -> {return_type.gen_type(imports=self.imports)}:"
        values = (p.gen_parameter_declaration(imports=self.imports) for p in params)
        self.add_aligned_line(start=f"def {function_name}(", end=end, separator=",", values=values)
        return PythonScope(self, make_on_close_empty_lines(Config.Format.EmptyLines.after_function))

    def gen_method_decl(self, method_name: str, params: Iterable[PythonVariable] = list(), 
                    return_type: Optional[PythonType] = None):
        self_param: list[PythonVariable] = [PythonSelfVariable()]
        child = self.gen_function_decl(function_name=method_name, 
                    params=self_param + list(params), return_type=return_type)
        child.cb = make_on_close_empty_lines(Config.Format.EmptyLines.after_method)
        return child

    def gen_static_method_decl(self, method_name: str, params: Iterable[PythonVariable] = list(), 
                    return_type: Optional[PythonType] = None):
        self.add_line("@staticmethod")
        return self.gen_function_decl(function_name=method_name, params=params, 
                    return_type=return_type)

    def gen_if(self, condition: str):
        self.add_line(f"if {condition}:")
        return PythonScope(self)

    def gen_elif(self, condition: str):
        self.add_line(f"elif {condition}:")
        return PythonScope(self)

    def gen_else(self):
        self.add_line(f"else:")
        return PythonScope(self)

    def gen_try(self):
        self.add_line(f"try:")
        return PythonScope(self)

    def gen_except(self, exception_typename: str = "Exception", exception_var: Optional[PythonVariable] = None):
        self.add_line(f"except {exception_typename}{f' as {exception_var}' if exception_var is not None else ''}:")
        return PythonScope(self)

    def gen_with(self, expression: str, variable: PythonVariable):
        self.add_line(f"with {expression} as {variable}:")
        return PythonScope(self)

    def gen_for(self, variables: list[PythonVariable], iterable: str):
        vars = f"({', '.join(v.name for v in variables)})" if len(variables) > 1 else variables[0].name
        self.add_line(f"for {vars} in {iterable}:")
        return PythonScope(self)

def make_on_close_empty_lines(n_empty_lines: int):
    def inner(script: PythonScript):
        script.empty_lines(n_empty_lines=n_empty_lines)
    return inner

class PythonScope:
    def __init__(self, script: PythonScript, cb: Optional[Callable[[PythonScript], None]] = None):
        self.script = script
        self.cb = cb

    def __enter__(self):
        self.script.add_indent(1)
        return self

    def __exit__(self, exit: object, value: object, exc: object):
        self.script.remove_indent(1)
        if self.cb is not None:
            self.cb(self.script)


class PythonImports:
    def __init__(self) -> None:
        self.__imports: dict[str, set[str]] = defaultdict(set)

    def add_import(self, module: str, object: str):
        self.__imports[module].add(object)

    def add_import_from_config(self, config_import: ConfigImport):
        self.add_import(module=config_import.module, object=config_import.object_name + 
                (f" as {config_import.alias}" if config_import.alias is not None else ""))

    def update(self, o: PythonImports):
        for (module, objects) in o.__imports.items():
            self.__imports[module].update(objects)

    def gen(self, script: PythonScript):
        for module, objects in self.__imports.items():
            script.add_line(f"from {module} import {', '.join(objects)}")
    
    def __len__(self):
        return len(self.__imports)

class PythonGlobals():
    def __init__(self) -> None:
        self.__vars: list[PythonVariable] = list()

    def add_global(self, var: PythonVariable):
        if not var.has_initial_value:
            raise Exception("Cannot add global variable without initial value")
        self.__vars.append(var)

    def gen(self, script: PythonScript):
        for var in self.__vars:
            var.gen_declarartion(script, True)

    def update(self, o: PythonGlobals):
        self.__vars.extend(o.__vars)

    def __len__(self):
        return len(self.__vars)



class PythonVariable:
    def __init__(self, name: str, type: Optional[PythonType] = None, initial_litteral: Optional[object] = None):
        self.name = name
        self.type = type  if type is not None else AnyPythonType()      
        self.initial_value = initial_litteral
        self.has_initial_value = initial_litteral is not None or self.type.is_optional() or self.type.is_none()

    def gen_parameter_declaration(self, imports: PythonImports, with_typing: bool = True):
        if not with_typing and not self.has_initial_value:
            raise Exception("Cannot declare without typing nor inital_value")
        initialisation = ""
        if self.has_initial_value:
                initialisation = " = " + self.type.gen_litteral(value=self.initial_value, 
                            imports=imports)
        typing = ""
        if with_typing:
            typing = ': ' + self.type.gen_type(imports)
        return f"{self.name}{typing}{initialisation}"

    def gen_declarartion(self, script: PythonScript, with_typing: bool = True):
        had_initial_value = True
        if not self.has_initial_value:
            had_initial_value = False
            self.initial_value = self.get_default_litteral()
            self.has_initial_value = True
        script.add_line(self.gen_parameter_declaration(imports=script.imports, 
                    with_typing=with_typing))
        if not had_initial_value:
            self.initial_value = None
            self.has_initial_value = False

    def get_default_litteral(self):
        if self.initial_value is not None:
            return self.initial_value
        return self.type.default_literal()

    def __str__(self):
        return self.name

class PythonField(PythonVariable):
    def __init__(self, name: str, type: Optional[PythonType] = None, 
                initial_litteral: Optional[object] = None):
        super().__init__(name, type=type, initial_litteral=initial_litteral)
        self.name = f"self.{self.name}"


class PythonSelfVariable(PythonVariable):
    def __init__(self):
        super().__init__("", PythonType(), initial_litteral=None)

    def gen_parameter_declaration(self, imports: PythonImports, with_typing: bool = True):
        return "self"

    def gen_variable_declarartion(self, imports: PythonImports) -> str:
        raise Exception("Cannot declare self variable")

class PythonType:
    def gen_type(self, imports: Optional[PythonImports] = None) -> str:
        raise Exception("Abstract method")

    def gen_litteral(self, value: object, imports: Optional[PythonImports] = None) -> str:
        return repr(value)

    def default_literal(self) -> object:
        raise Exception("Abstract method")

    def litteral_from_str(self, s: str) -> object:
        raise Exception("Abstract method")

    def is_valid_litteral(self, value: object) -> bool:
        try:
            self.gen_litteral(value)
            return True
        except Exception:
            return False

    def is_valid_str_litteral(self, s: str) -> bool:
        try:
            self.litteral_from_str(s)
            return True
        except Exception:
            return False
            
    def is_super_type(self, o: PythonType) -> bool:
        return isinstance(o, type(self)) \
            or (o.is_enum() and not o.is_optional() and self.is_super_type(o.get_raw_type()))

    def is_optional(self) -> bool:
        return False

    def is_none(self) -> bool:
        return False

    def is_enum(self) -> bool:
        return False

    def enum_values(self) -> list[object]:
        raise Exception(f"{self} is not an enum")

    def enum_raw_type(self) -> PythonType:
        raise Exception(f"{self} is not an enum")

    def is_text(self) -> bool:
        return False

    def __eq__(self, o: object) -> bool:
        return isinstance(o, type(self))

    def __hash__(self) -> int:
        return hash(type(self))
    
    def litteral_conversion_error(self, value: object, expected_type: Optional[str] = None):
        raise Exception(f"Cannot generate {self.gen_type()} litteral from {value} of type {type(value)}"
         + f" (is not {expected_type})" if expected_type is not None else "")

    def __str__(self):
        return self.gen_type()

    def get_raw_type(self):
        return self

class OptionalPythonType(PythonType):
    def __init__(self, inner_type: PythonType):
        super().__init__()
        assert(not inner_type.is_none())
        assert(not inner_type.is_optional())
        self.__inner_type = inner_type

    def gen_type(self, imports: Optional[PythonImports] = None) -> str:
        if imports is not None:
            imports.add_import(module="typing", object="Optional")
        return f"Optional[{self.__inner_type.gen_type(imports=imports)}]"

    def gen_litteral(self, value: object, imports: Optional[PythonImports] = None) -> str:
        if value is None:
            return super().gen_litteral(value, imports)
        else:
            return self.__inner_type.gen_litteral(value=value, imports=imports)

    def litteral_from_str(self, s: str) -> object:
        if s == "None":
            return None
        return self.__inner_type.litteral_from_str(s)

    def default_literal(self) -> object:
        return None

    def is_super_type(self, o: PythonType):
        if isinstance(o, OptionalPythonType):
            o = o.__inner_type
        return o.is_none() or self.__inner_type.is_super_type(o)

    def __eq__(self, o: object) -> bool:
        return isinstance(o, OptionalPythonType) and self.__inner_type == o.__inner_type

    def __hash__(self) -> int:
        return  super().__hash__() + hash(self.__inner_type)
    
    def is_optional(self):
        return True

    def is_text(self) -> bool:
        return self.__inner_type.is_text()

    def is_enum(self) -> bool:
        return self.__inner_type.is_enum()

    def enum_raw_type(self) -> PythonType:
        return OptionalPythonType(self.__inner_type.enum_raw_type())

    def enum_values(self) -> list[object]:
        return self.__inner_type.enum_values()

    def get_raw_type(self):
        return self.__inner_type


class BaseClassPythonType(PythonType):
    def __init__(self, class_name: str, parent: Optional[ClassPythonType] = None):
            super().__init__()
            self.__class_name = class_name
            self.__parent = parent

    def gen_type(self, imports: Optional[PythonImports] = None) -> str:
        return self.__class_name

    def is_super_type(self, o: PythonType) -> bool:
        raise NotImplementedError()

    def get_parent(self):
        return self.__parent

    def __eq__(self, o: object) -> bool:
        return isinstance(o, BaseClassPythonType) and self.__class_name == o.__class_name and self.__parent == o.__parent

    def __hash__(self) -> int:
        return super().__hash__() + hash(self.__class_name) + hash(self.__parent)

class ClassPythonType(BaseClassPythonType):
    def __init__(self, class_name: str, parent: Optional[ClassPythonType] = None):
        super().__init__(class_name, parent)

    def gen_litteral(self, value: object, imports: Optional[PythonImports] = None) -> str:
        value = cast(list[str], value)
        if not isinstance(value, list) or not all(isinstance(e, str) for e in value):
            self.litteral_conversion_error(value, expected_type="list[str]")
        return f"{self.gen_type(imports)}({', '.join(value)})"

    def default_literal(self) -> list[str]:
        return []

    def litteral_from_str(self, s: str) -> object:
        raise NotImplementedError()


class IntPythonType(PythonType):
    def __init__(self) -> None:
        super().__init__()

    def gen_type(self, imports: Optional[PythonImports] = None) -> str:
        return "int"

    def gen_litteral(self, value: object, imports: Optional[PythonImports] = None) -> str:
        if not isinstance(value, int):
            self.litteral_conversion_error(value)
        return super().gen_litteral(value, imports)

    def default_literal(self) -> int:
        return 0

    def litteral_from_str(self, s: str) -> int:
        return int(s)


class FloatPythonType(PythonType):
    def __init__(self) -> None:
        super().__init__()

    def gen_type(self, imports: Optional[PythonImports] = None) -> str:
        return "float"

    def gen_litteral(self, value: object, imports: Optional[PythonImports] = None) -> str:
        if not isinstance(value, float) and not isinstance(value, int):
            self.litteral_conversion_error(value, expected_type="float or int")
        return super().gen_litteral(value, imports)

    def default_literal(self) -> float:
        return 0

    def litteral_from_str(self, s: str) -> float:
        return float(s)

    def is_super_type(self, o: PythonType) -> bool:
        return super().is_super_type(o) or isinstance(o, IntPythonType)


class StrPythonType(PythonType):
    def __init__(self) -> None:
        super().__init__()

    def gen_type(self, imports: Optional[PythonImports] = None) -> str:
        return "str"

    def gen_litteral(self, value: object, imports: Optional[PythonImports] = None) -> str:
        if not isinstance(value, str):
            self.litteral_conversion_error(value)
        return super().gen_litteral(value, imports)

    def default_literal(self) -> str:
        return ""

    def litteral_from_str(self, s: str) -> str:
        return s

    def is_text(self) -> bool:
        return True

class TimePythonType(PythonType):
    def __init__(self) -> None:
        super().__init__()

    def gen_type(self, imports: Optional[PythonImports] = None) -> str:
        if imports is not None:
            imports.add_import(module="datetime", object="time")
        return "time"

    def gen_litteral(self, value: object, imports: Optional[PythonImports] = None) -> str:
        if not isinstance(value, time):
            self.litteral_conversion_error(value)
        if imports is not None:
            imports.add_import(module="datetime", object="time")
        return repr(value).partition(".")[2] # to get rid of datetime.

    def default_literal(self) -> time:
        return time(1, 1, 1)

    def litteral_from_str(self, s: str) -> datetime:
        return datetime.strptime(s, "%H:%M:%S")

    
class DateTimePythonType(PythonType):
    def __init__(self) -> None:
        super().__init__()

    def gen_type(self, imports: Optional[PythonImports] = None) -> str:
        if imports is not None:
            imports.add_import(module="datetime", object="datetime")
        return "datetime"

    def gen_litteral(self, value: object, imports: Optional[PythonImports] = None) -> str:
        if not isinstance(value, datetime):
            self.litteral_conversion_error(value)
        if imports is not None:
            imports.add_import(module="datetime", object="datetime")
        return repr(value).partition(".")[2] # to get rid of datetime.

    def default_literal(self) -> datetime:
        return datetime(1, 1, 1)
    
    def litteral_from_str(self, s: str) -> datetime:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S") 

class BaseEnumPythonType(PythonType):
    def __init__(self, inner_type: PythonType) -> None:
        self.__inner_type = inner_type
        super().__init__()

    def gen_type(self, imports: Optional[PythonImports] = None) -> str:
        if imports is not None:
            raise Exception("BaseEnum is an abstract type, it cannot be generated to python code")
        return f"Enum[{self.__inner_type}]"

    def is_super_type(self, o: PythonType) -> bool:
        return o.is_enum() and self.__inner_type.is_super_type(o.get_raw_type())

    def is_enum(self) -> bool:
        return True

    def enum_raw_type(self) -> PythonType:
        return self.__inner_type

    def is_text(self) -> bool:
        return self.__inner_type.is_text()
    
    def get_raw_type(self):
        return self.__inner_type

class EnumPythonType(BaseEnumPythonType):
    def __init__(self, inner_type: PythonType, values: Iterable[object]):
        super().__init__(inner_type)
        self.__values = list(values)

    def gen_type(self, imports: Optional[PythonImports] = None) -> str:
        if len(self.__values) == 0:
            if imports is not None:
                raise Exception("Cannot generate enum with no value")
            return "NoReturn"
        if imports is not None:
            imports.add_import(module="typing", object="Literal")
        gen = self.get_raw_type().gen_litteral
        if len(self.__values) == 1:
            return f"Literal[{gen(self.__values[0], imports)}]"
        if imports is not None:
            imports.add_import(module="typing", object="Union")
        return f"Union[{', '.join(f'Literal[{gen(v, imports)}]' for v in self.__values)}]"

    def gen_litteral(self, value: object, imports: Optional[PythonImports] = None) -> str:
        return self.get_raw_type().gen_litteral(value)

    def default_literal(self) -> object:
        return self.__values[0]

    def litteral_from_str(self, s: str) -> str:
        if s in self.__values:
            return s
        raise Exception(f"Cannot parse {s} to {self.gen_type()}")

    def is_super_type(self, o: PythonType) -> bool:
        return o.is_enum() and set(o.enum_values()).issubset(set(self.__values))

    def __eq__(self, o: object) -> bool:
        return isinstance(o, EnumPythonType) and set(self.__values) == set(o.__values)

    def __hash__(self) -> int:
        return super().__hash__() + hash(self.__values)

    def enum_values(self):
        return self.__values

class BytesPythonType(PythonType):
    def __init__(self) -> None:
        super().__init__()

    def gen_type(self, imports: Optional[PythonImports] = None) -> str:
        return "bytes"

    def gen_litteral(self, value: object, imports: Optional[PythonImports] = None) -> str:
        if not isinstance(value, bytes):
            self.litteral_conversion_error(value)
        return repr(value)

    def default_literal(self) -> bytes:
        return bytes()

    def litteral_from_str(self, s: str) -> bytes:
        raise NotImplementedError()


class AnyPythonType(PythonType):
    def __init__(self) -> None:
        super().__init__()
    
    def gen_type(self, imports: Optional[PythonImports] = None) -> str:
        if imports is not None:
            imports.add_import(module="typing", object="Any")
        return "Any"

    def gen_litteral(self, value: object, imports: Optional[PythonImports] = None) -> str:
        raise NotImplementedError()

    def default_literal(self) -> object:
        raise NotImplementedError()

    def litteral_from_str(self, s: str) -> object:
        raise NotImplementedError()

    def is_super_type(self, o: PythonType) -> bool:
        return True

class IterablePythonType(PythonType):
    def __init__(self, inner_type: PythonType) -> None:
        super().__init__()
        self.__inner_type = inner_type

    def gen_type(self, imports: Optional[PythonImports] = None) -> str:
        if imports is not None:
            imports.add_import(module="typing", object="Iterable")
        return f"Iterable[{self.__inner_type.gen_type(imports=imports)}]"

    def is_super_type(self, o: PythonType) -> bool:
        return isinstance(o, type(self)) and self.__inner_type.is_super_type(o.__inner_type)

    def __eq__(self, o: object) -> bool:
        return isinstance(o, type(self)) and self.__inner_type == o.__inner_type

    def __hash__(self) -> int:
        return super().__hash__() + hash(self.__inner_type)

    def get_raw_type(self):
        return self.__inner_type


class ListPythonType(IterablePythonType):
    def __init__(self, inner_type: PythonType) -> None:
        super().__init__(inner_type=inner_type)

    def gen_type(self, imports: Optional[PythonImports] = None) -> str:
        return f"list[{self.get_raw_type().gen_type(imports=imports)}]"

    def gen_litteral(self, value: object, imports: Optional[PythonImports] = None) -> str:
        value = cast(Iterable[object], object)
        if not isinstance(value, Iterable):
            self.litteral_conversion_error(value, expected_type="Iterable")
        return repr(list(self.get_raw_type().gen_litteral(v, imports) for v in value))

    def default_literal(self) -> list[object]:
        return []

    def litteral_from_str(self, s: str) -> list[object]:
        raise NotImplementedError()


class DictPythonType(IterablePythonType):
    def __init__(self, key_type: PythonType, value_type: PythonType) -> None:
        super().__init__(key_type)
        self.__value_type = value_type

    def gen_type(self, imports: Optional[PythonImports] = None) -> str:
        return f"dict[{self.get_raw_type().gen_type(imports=imports)}, " \
                    f"{self.__value_type.gen_type(imports=imports)}]"

    def gen_litteral(self, value: object, imports: Optional[PythonImports] = None) -> str:
        value = cast(dict[object, object], value)
        if not isinstance(value, dict):
            print("pas ok", value, type(value))
            self.litteral_conversion_error(value)
        return "{" + ", ".join({self.get_raw_type().gen_litteral(k, imports) + ":" + self.__value_type.gen_litteral(v, imports) 
                    for (k, v) in value.items()}) + "}"
       
    def default_literal(self) -> object:
        return {self.get_raw_type().default_literal(): self.__value_type.default_literal()}

    def litteral_from_str(self, s: str) -> object:
        dict_literal = cast(dict[object, object], literal_eval(s))
        return isinstance(dict_literal, dict) \
            and all(self.get_raw_type().is_valid_litteral(k) 
                    and self.__value_type.is_valid_litteral(v) 
                    for (k, v) in dict_literal.items())

    def is_super_type(self, o: PythonType) -> bool:
        return super().is_super_type(o) and isinstance(o, type(self)) and self.__value_type.is_super_type(o.__value_type)

    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and isinstance(o, type(self)) and self.__value_type == o.__value_type

    def __hash__(self) -> int:
        return super().__hash__() + hash(self.__value_type)

class NonePythonType(PythonType):
    def __init__(self) -> None:
        super().__init__()

    def gen_type(self, imports: Optional[PythonImports] = None) -> str:
        return "None"

    def gen_litteral(self, value: object, imports: Optional[PythonImports] = None) -> str:
        if value is not None:
            self.litteral_conversion_error(value)
        return "None"

    def default_literal(self) -> object:
        return None

    def litteral_from_str(self, s: str) -> object:
        if s.strip() != "None":
            raise Exception(f"Cannot parse {s} to None")
        return None

    def is_none(self) -> bool:
        return True
    
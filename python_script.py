from __future__ import annotations
from collections import defaultdict
from typing import Any, Callable, Iterable, Optional
from code_gen_config import Config, ConfigImport
from util.util import extract_delimiter
from datetime import datetime

class PythonScript:

    def __init__(self, parent_script: Optional[PythonScript] = None, on_close: Optional[Callable[[PythonScript], None]] = None):
        self.__script = ""
        self.__current_indent = 0
        self.current_prefix = ""
        self.imports = PythonImports()
        self.imports.add_import(module="__future__", object="annotations")
        self.global_variables: list[PythonVariable] = list()
        self.parent_script = parent_script
        self.on_close = on_close

    def add_line(self, line: str):
        self.__script += self.current_prefix + line + "\n"

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

    def add_global_variable(self, variable: PythonVariable):
        self.global_variables.append(variable)

    def __add_imports_to_script(self):
        if len(self.imports) > 0:
            self.__script = self.imports.gen() \
                            + Config.Format.EmptyLines.after_imports * "\n" + self.__script

    def get_script(self):
        self.__add_imports_to_script()
        return self.__script

    def add_sub_script(self, other: PythonScript):
        self.imports.update(other.imports)
        self.global_variables.extend(other.global_variables)
        self.__script += other.__script

    def __enter__(self):
        self.add_indent(1)
        return self

    def __exit__(self, exit: Any, value: Any, exc: Any):
        self.remove_indent(1)
        if self.on_close is not None:
            self.on_close(self)
        if self.parent_script is None:
            raise Exception("Cannot exit script without parent")
        self.parent_script.add_sub_script(self)

    def __new_scope(self, on_close: Optional[Callable[[PythonScript], None]] = None):
        child = PythonScript(parent_script=self, on_close=on_close)
        child.add_indent(value=self.__current_indent)
        return child

    def gen_class_decl(self, class_type: BaseClassPythonType):
        class_name = class_type.gen_type(self.imports)
        parent = f"({class_type.parent.gen_type(self.imports)})" if class_type.parent is not None else ''
        self.add_line(line=f"class {class_name}{parent}:")
        return self.__new_scope(on_close=make_on_close_empty_lines(Config.Format.EmptyLines.after_class))

    def gen_function_decl(self, function_name: str, params: Iterable[PythonVariable] = list(), 
                    return_type: Optional[PythonType] = None):
        end = '):'
        if return_type is not None:
            end = f") -> {return_type.gen_type(imports=self.imports)}:"
        values = (p.gen_parameter_declaration(imports=self.imports) for p in params)
        self.add_aligned_line(start=f"def {function_name}(", end=end, separator=",", values=values)
        return self.__new_scope(on_close=make_on_close_empty_lines(Config.Format.EmptyLines.after_function))

    def gen_method_decl(self, method_name: str, params: Iterable[PythonVariable] = list(), 
                    return_type: Optional[PythonType] = None):
        self_param: list[PythonVariable] = [PythonSelfVariable()]
        child = self.gen_function_decl(function_name=method_name, 
                    params=self_param + list(params), return_type=return_type)
        child.on_close = make_on_close_empty_lines(Config.Format.EmptyLines.after_method)
        return child

    def gen_static_method_decl(self, method_name: str, params: Iterable[PythonVariable] = list(), 
                    return_type: Optional[PythonType] = None):
        self.add_line("@staticmethod")
        return self.gen_function_decl(function_name=method_name, params=params, 
                    return_type=return_type)

    def gen_if(self, condition: str):
        self.add_line(f"if {condition}:")
        return self.__new_scope()

    def gen_try(self):
        self.add_line(f"try:")
        return self.__new_scope()

    def gen_except(self, exception_varname: str = ""):
        self.add_line(f"except Exception{f' as {exception_varname}' if len(exception_varname) > 0 else ''}:")
        return self.__new_scope()

def make_on_close_empty_lines(n_empty_lines: int):
    def inner(script: PythonScript):
        script.empty_lines(n_empty_lines=n_empty_lines)
    return inner

        
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

    def gen(self):
        return "\n".join(f"from {module} import {', '.join(objects)}"
                         for module, objects in self.__imports.items()) + "\n"
    
    def __len__(self):
        return len(self.__imports)


class PythonVariable:
    def __init__(self, name: str, type: Optional[PythonType] = None, initial_litteral: Optional[Any] = None,
                 initial_value_if_optional: bool = True):
        self.name = name
        self.type = type        
        self.initial_value = initial_litteral
        self.has_initial_value = initial_litteral is not None
        if type is not None and not self.has_initial_value and type.is_optional() \
                    and initial_value_if_optional:
            self.has_initial_value = True

    def gen_parameter_declaration(self, imports: PythonImports, with_typing: bool = True):
        if self.type is None:
            raise Exception("Cannot declare without type")
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
        if not self.has_initial_value:
            raise Exception("Cannot create variable declaration without initial value")
        script.add_line(self.gen_parameter_declaration(imports=script.imports, 
                    with_typing=with_typing))

    def get_default_litteral(self):
        if self.initial_value is not None:
            return self.initial_value
        elif self.type is not None:
            return self.type.default_literal()
        else:
            raise Exception("Cannot generate default literral")

    def __str__(self):
        return self.name

class PythonField(PythonVariable):
    def __init__(self, name: str, type: Optional[PythonType] = None, 
                initial_litteral: Optional[Any] = None, initial_value_if_optional: bool = True):
        super().__init__(name, type=type, initial_litteral=initial_litteral, 
                    initial_value_if_optional=initial_value_if_optional)
        self.name = f"self.{self.name}"


class PythonSelfVariable(PythonVariable):
    def __init__(self):
        super().__init__("", PythonType(), initial_litteral=None)

    def gen_parameter_declaration(self, imports: PythonImports, with_typing: bool = True):
        return "self"

    def gen_variable_declarartion(self, imports: PythonImports) -> str:
        raise Exception("Cannot declare self variable")


class PythonType:
    def gen_type(self, imports: PythonImports) -> str:
        raise Exception("Abstract method")

    def gen_litteral(self, value: Any, imports: PythonImports) -> str:
        raise Exception("Abstract method")

    def __eq__(self, o: object) -> bool:
        return type(self) == type(o)

    def is_super_type(self, o: PythonType) -> bool:
        raise Exception("Abstract method")

    def is_optional(self) -> bool:
        return False

    def is_none(self) -> bool:
        return False

    def default_literal(self) -> Any:
        raise Exception("Abstract method")

    def is_valid_str_litteral(self, s: str) -> bool:
        raise Exception("Abstract method")

    def __hash__(self) -> int:
        raise Exception("Abstract method")


class OptionalPythonType(PythonType):
    def __init__(self, inner_type: PythonType):
        super().__init__()
        assert(not isinstance(inner_type, OptionalPythonType))
        self.inner_type = inner_type

    def gen_type(self, imports: PythonImports) -> str:
        imports.add_import(module="typing", object="Optional")
        return f"Optional[{self.inner_type.gen_type(imports=imports)}]"

    def gen_litteral(self, value: Optional[Any], imports: PythonImports) -> str:
        if value is None:
            return "None"
        else:
            return self.inner_type.gen_litteral(value=value, imports=imports)
    
    def is_optional(self):
        return True

    def is_super_type(self, o: PythonType):
        return o.is_none() or self.inner_type.is_super_type(o)

    def default_literal(self) -> Optional[Any]:
        return None

    def is_valid_str_litteral(self, s: str):
        return s == "None" or self.inner_type.is_valid_str_litteral(s)

    def __hash__(self) -> int:
        return hash("Optional")



class BaseClassPythonType(PythonType):
    def __init__(self, class_name: str, parent: Optional[ClassPythonType] = None):
            super().__init__()
            self.class_name = class_name
            self.parent = parent

    def gen_type(self, imports: PythonImports) -> str:
        return self.class_name

    def is_super_type(self, o: PythonType) -> bool:
        raise NotImplementedError()

class ClassPythonType(BaseClassPythonType):
    def __init__(self, class_name: str, parent: Optional[ClassPythonType] = None):
        super().__init__(class_name, parent)

    def gen_litteral(self, value: list[str], imports: PythonImports) -> str:
        return f"{self.class_name}({', '.join(value)})"

    def default_literal(self) -> list[str]:
        return []

    def is_valid_str_litteral(self, s: str) -> bool:
        raise NotImplementedError()

    def __hash__(self) -> int:
        return hash("Class")

class IntPythonType(PythonType):
    def __init__(self) -> None:
        super().__init__()

    def gen_type(self, imports: PythonImports) -> str:
        return "int"

    def gen_litteral(self, value: int, imports: PythonImports) -> str:
        return str(value)

    def is_super_type(self, o: PythonType) -> bool:
        return self == o

    def default_literal(self) -> int:
        return 0

    def is_valid_str_litteral(self, s: str) -> bool:
        try:
            int(s)
            return True
        except Exception:
            return False

    def __hash__(self) -> int:
        return hash("Int")


class FloatPythonType(PythonType):
    def __init__(self) -> None:
        super().__init__()

    def gen_type(self, imports: PythonImports) -> str:
        return "float"

    def gen_litteral(self, value: float, imports: PythonImports) -> str:
        return str(value)

    def is_super_type(self, o: PythonType) -> bool:
        return self == o or IntPythonType().is_super_type(o)

    def default_literal(self) -> float:
        return 0

    def is_valid_str_litteral(self, s: str) -> bool:
        try:
            float(s)
            return True
        except Exception:
            return False

    def __hash__(self) -> int:
        return hash("Float")

class StrPythonType(PythonType):
    def __init__(self) -> None:
        super().__init__()

    def gen_type(self, imports: PythonImports) -> str:
        return "str"

    def gen_litteral(self, value: str, imports: PythonImports) -> str:
        return f'"{value}"'

    def is_super_type(self, o: PythonType) -> bool:
        return self == o

    def default_literal(self) -> str:
        return ""

    def is_valid_str_litteral(self, s: str) -> bool:
        return True

    def __hash__(self) -> int:
        return hash("Str")

class TimePythonType(PythonType):
    def __init__(self) -> None:
        super().__init__()

    def gen_type(self, imports: PythonImports) -> str:
        imports.add_import(module="datetime", object="time")
        return "time"

    def gen_litteral(self, value: datetime, imports: PythonImports) -> str:
        imports.add_import(module="datetime", object="time")
        return f"time(hour={value.hour}, minute={value.minute}, second={value.second})"

    def is_super_type(self, o: PythonType) -> bool:
        return self == o

    def default_literal(self) -> datetime:
        return datetime(1, 1, 1)

    def is_valid_str_litteral(self, s: str) -> bool:
        try:
            datetime.strptime(s, "%H:%M:%S") 
            return True
        except: 
            return False

    def __hash__(self) -> int:
        return hash("Time")
    
class DateTimePythonType(TimePythonType):
    def __init__(self) -> None:
        super().__init__()

    def gen_type(self, imports: PythonImports) -> str:
        imports.add_import(module="datetime", object="datetime")
        return "datetime"

    def gen_litteral(self, value: datetime, imports: PythonImports) -> str:
        imports.add_import(module="datetime", object="datetime")
        timepart = extract_delimiter(super().gen_litteral(value=value, imports=imports))
        return f"datetime(year={value.year}, month={value.month}, day={value.day}, {timepart})"

    def is_super_type(self, o: PythonType) -> bool:
        return self == o

    def is_valid_str_litteral(self, s: str) -> bool:
        try:
            datetime.strptime(s, "%Y-%m-%d %H:%M:%S") 
            return True
        except: 
            return False

    def __hash__(self) -> int:
        return hash("DateTime")

class EnumPythonType(StrPythonType):
    def __init__(self, values: list[str]):
        super().__init__()
        self.values = values

    def gen_type(self, imports: PythonImports) -> str:
        imports.add_import(module="typing", object="Literal")
        imports.add_import(module="typing", object="Union")
        foo = super().gen_litteral
        return f"Union[{', '.join(f'Literal[{foo(v, imports)}]' for v in self.values)}]"

    def gen_litteral(self, value: str, imports: PythonImports) -> str:
        if value not in self.values:
            raise Exception(f"Invalid value enum {value} for enum {', '.join}")
        return super().gen_litteral(value=value, imports=imports)

    def is_super_type(self, o: PythonType) -> bool:
        return self == o or StrPythonType().is_super_type(o)

    def default_literal(self) -> str:
        return self.values[0]

    def is_valid_str_litteral(self, s: str) -> bool:
        return s in self.values

    def __hash__(self) -> int:
        return hash("enum")

class BytesPythonType(PythonType):
    def __init__(self) -> None:
        super().__init__()

    def gen_type(self, imports: PythonImports) -> str:
        return "bytes"

    def gen_litteral(self, value: bytes, imports: PythonImports) -> str:
        return str(value)

    def is_super_type(self, o: PythonType) -> bool:
        return self == o

    def default_literal(self) -> bytes:
        return bytes()

    def is_valid_str_litteral(self, s: str) -> bool:
        raise NotImplementedError()

    def __hash__(self) -> int:
        return hash("Bytes")


class AnyPythonType(PythonType):
    def __init__(self) -> None:
        super().__init__()
    
    def gen_type(self, imports: PythonImports) -> str:
        imports.add_import(module="typing", object="Any")
        return "Any"

    def gen_litteral(self, value: bytes, imports: PythonImports) -> str:
        raise NotImplementedError()

    def is_super_type(self, o: PythonType) -> bool:
        return True

    def default_literal(self) -> Any:
        raise NotImplementedError()

    def is_valid_str_litteral(self, s: str) -> bool:
        raise NotImplementedError()

    def __hash__(self) -> int:
        return hash("Any")

class IterablePythonType(PythonType):
    def __init__(self, inner_type: PythonType) -> None:
        super().__init__()
        self.inner_type = inner_type

    def gen_type(self, imports: PythonImports) -> str:
        imports.add_import(module="typing", object="Iterable")
        return f"Iterable[{self.inner_type.gen_type(imports=imports)}]"

    def gen_litteral(self, value: Any, imports: PythonImports) -> str:
        raise Exception("Abstract method")

    def is_super_type(self, o: PythonType) -> bool:
        return issubclass(type(o), type(self))

    def default_literal(self) -> Any:
        raise Exception("Abstract method")

    def is_valid_str_litteral(self, s: str) -> bool:
        raise NotImplementedError()

class ListPythonType(IterablePythonType):
    def __init__(self, inner_type: PythonType) -> None:
        super().__init__(inner_type=inner_type)

    def gen_type(self, imports: PythonImports) -> str:
        return f"list[{self.inner_type.gen_type(imports=imports)}]"

    def gen_litteral(self, value: Iterable[Any], imports: PythonImports) -> str:
        return f"[{', '.join(self.inner_type.gen_litteral(value=value, imports=imports))}]"

    def default_literal(self) -> list[Any]:
        return []

    def __hash__(self) -> int:
        return hash(self.inner_type)

class DictPythonType(IterablePythonType):
    def __init__(self, key_type: PythonType, value_type: PythonType) -> None:
        super().__init__(key_type)
        self.value_type = value_type

    def gen_type(self, imports: PythonImports) -> str:
        return f"dict[{self.inner_type.gen_type(imports=imports)}, " \
                    f"{self.value_type.gen_type(imports=imports)}]"

    def gen_litteral(self, value: dict[Any, Any], imports: PythonImports) -> str:
        return "{" + ', '.join(self.inner_type.gen_litteral(value=k, imports=imports) + 
                         ":" + self.value_type.gen_litteral(value=v, imports=imports) 
                            for (k, v) in value.items()) + "}"
                    
    def default_literal(self) -> Any:
        return {}

    def __hash__(self) -> int:
        return hash(self.inner_type) + hash(self.value_type)

class NonePythonType(PythonType):
    def __init__(self) -> None:
        super().__init__()

    def gen_type(self, imports: PythonImports) -> str:
        return "None"

    def gen_litteral(self, value: None, imports: PythonImports) -> str:
        return "None"

    def is_none(self) -> bool:
        return True

    def is_super_type(self, o: PythonType) -> bool:
        return self == o

    def default_literal(self) -> Any:
        return None

    def is_valid_str_litteral(self, s: str) -> bool:
        return s == "None"

    def __hash__(self) -> int:
        return hash("None")

    
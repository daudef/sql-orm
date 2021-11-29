from typing import Any
from uuid import uuid4
from random import choice


def parse_int(s: str):
    return int(s)

def parse_float(s: str):
    return float(s)

def parse_bool(s: str):
    return bool(s)

def stringify_int(i: int):
    return str(i)

def stringify_float(i: float):
    return str(i)

def stringify_bool(i: bool):
    return str(i)

def lower_str(s: str):
    return s.lower()

def upper_str(s: str):
    return s.upper()

def uuid4_generator(_: Any):
    return str(uuid4())

def hex_color_generator(_: Any):
    return "#" + "".join(choice("0123456789abcdef") for _ in range(6))




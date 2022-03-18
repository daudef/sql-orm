from typing import Optional
from uuid import uuid4
from random import choice


def parse_int(s: str):
    return int(s)

def parse_opt_int(s: Optional[str]):
    if s is None:
        return None
    return parse_int(s)

def parse_float(s: str):
    return float(s)

def parse_opt_float(s: Optional[str]):
    if s is None:
        return None
    return parse_float(s)

def parse_bool(s: str):
    return bool(s)

def parse_opt_bool(s: str):
    if s is None:
        return None
    return parse_bool(s)

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

def nullify_str(s: str):
    if len(s.strip()) == 0:
        return None
    return s

def constant_str(value: str):
    def inner(_: None):
        return value
    return inner

def uuid4_generator(_: None):
    return str(uuid4())

def hex_color_generator(_: None):
    return "#" + "".join(choice("0123456789abcdef") for _ in range(6))

SPECIAL_CHAR_MAP = {
    192: "A",
    193: "A",
    194: "A",
    195: "A",
    196: "A",
    197: "A",
    198: "AE",
    199: "C",
    200: "E",
    201: "E",
    202: "E",
    203: "E",
    204: "I",
    205: "I",
    206: "I",
    207: "I",
    208: "D",
    209: "N",
    210: "O",
    211: "O",
    212: "O",
    213: "O",
    214: "O",
    216: "O",
    217: "U",
    218: "U",
    219: "U",
    220: "U",
    221: "Y",
    223: "B",
    224: "a",
    225: "a",
    226: "a",
    227: "a",
    228: "a",
    229: "a",
    230: "ae",
    231: "c",
    232: "e",
    233: "e",
    234: "e",
    235: "e",
    236: "i",
    237: "i",
    238: "i",
    239: "i",
    241: "n",
    242: "o",
    243: "o",
    244: "o",
    245: "o",
    246: "o",
    249: "u",
    250: "u",
    251: "u",
    252: "u",
    253: "y",
    255: "y",
}

def to_ascii(s: str):
    def to_ascii_char(c: str):
        ascii_num = ord(c)
        if ascii_num < 128:
            return c
        elif 128 <= ascii_num < 192 or 255 <= ascii_num:
            return " "
        else:
            return SPECIAL_CHAR_MAP[ascii_num]
    return " ".join("".join(to_ascii_char(c) for c in s).split())



def enum_converter(enum_asso: dict[str, str]):
    def inner(enum: str):
        return enum_asso[enum]
    return inner


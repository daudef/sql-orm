from typing import Callable, Iterable, Iterator, Optional, TypeVar, cast


def btos(bytes: bytes):
    return "".join(map(chr, bytes))

def extract_delimiter(s: str, start: str = "(", end: str = ")"):
    return end.join(start.join(s.split(start)[1:]).split(end)[:-1])

def snake_case_to_camel_case(snake_case: str):
    camel_case = ""
    next_is_capital = True
    for c in snake_case:
        if c == "_" and not next_is_capital:
            next_is_capital = True
            continue
        elif next_is_capital:
            camel_case += c.upper()
            next_is_capital = False
        else:
            camel_case += c
    return camel_case 

T = TypeVar('T')

def pairs(it: Iterable[T]) -> Iterator[tuple[T, T]]:
    '''Iterate over pairs of consecutive elements'''
    previous = None
    for e in it:
        if previous is not None:
            yield (previous, e)
        previous = e  

__DEFAULT_PROMPT = "Choice: "
__DEFAULT_INVALID_MESSAGE = "Invalid choice"

def choose(items: list[T], prompt: str = __DEFAULT_PROMPT, default: Optional[T] = None, default_str: Optional[str] = None) -> T:
    if default is not None and default not in items:
        items.append(default)
    if len(items) == 0:
        raise Exception("Cannot choose from no item")
    if len(items) == 1:
        return items[0]
    default_prompt = (default_str if default_str is not None else str(default)) if default else ""
    while (True):
        try:
            user_input = input(prompt + (f" (default: {items.index(default)} - {default_prompt})" if default else "") + ": ")
            if user_input == "":
                if default is not None:
                    return cast(T, default)
                else:
                    raise Exception
            if 0 <= (choice := int(user_input)) < len(items):
                return items[choice]
        except Exception:
            pass
        print(__DEFAULT_INVALID_MESSAGE)

def print_choose(items: list[T], printer: Callable[[T], str], prompt: str = __DEFAULT_PROMPT, 
                suffix: str = "", default: Optional[T] = None, prefix: str = "") -> T:
    if default is not None and default not in items:
        items.append(default)
    if len(items) == 0:
        raise Exception("Cannot choose from no item")
    if len(items) == 1:
        return items[0] 
    print(prefix, end="")
    for i, item in enumerate(items):
        print(f" {i} - {printer(item)}")
    choice = choose(items, prompt, default=default, default_str=printer(default) if default is not None else None)
    print(suffix, end="")
    return choice

def print_choose_str(items: list[str], prompt: str = __DEFAULT_PROMPT, suffix: str = "", 
            default: Optional[str] = None, prefix: str = "") -> str: 
    return print_choose(items=items, printer=lambda s: s, prompt=prompt, suffix=suffix, 
                default=default, prefix=prefix) 


def yes_no_choose(prompt: str = __DEFAULT_PROMPT, prefix: str = "", suffix: str = "", default: Optional[bool] = None):
    print(prefix, end="")
    
    while True:
        choice = input(f"{prompt} ({'Y' if default == True else 'y'}/{'N' if default == False else 'n'}) ")
        if choice == "" and default is not None:
            res = default
            break
        elif choice.lower() == 'y':
            res = True
            break
        elif choice.lower() == 'n':
            res = False
            break
        print(__DEFAULT_INVALID_MESSAGE)
    
    print(suffix, end="")
    return res


def free_input(prompt: str = __DEFAULT_PROMPT, prefix: str = "", suffix: str = "", 
               default: Optional[str] = None, validator: Callable[[str], bool] = lambda s: True, 
               invalid_message: str = __DEFAULT_INVALID_MESSAGE):
    print(prefix, end="")
    while True:
        choice = input(f"{prompt}{f' (default {default}) ' if default is not None else ''}: ")
        if validator(choice):
            break
        print(invalid_message)
    print(suffix, end="")
    return choice


INSERT_COST = 2
MATCH_BONUS = 1
MISMATCH_COST = 1

def str_similarity(s1: str, s2: str):
    def word_similarity(w1: str, w2: str):
        cost_matrix: list[list[float]] = \
                    list(list(0.0 for _ in range(len(w2)+1)) for _ in range(len(w1) + 1))
        for (i, c1) in enumerate(w1):
            for (j, c2) in enumerate(w2):
                cost_matrix[i+1][j+1] = max(
                        cost_matrix[i][j+1] - INSERT_COST, 
                        cost_matrix[i+1][j] - INSERT_COST,
                        cost_matrix[i][j] + MATCH_BONUS if c1 == c2 else -MISMATCH_COST)
        max_cost = max(max(l) for l in cost_matrix)
        return max_cost if max_cost > 2 else -0.25
    def to_words(s: str):
        return "".join(c.lower() if 96 < ord(c.lower()) < 123 else " " for c in s).split()
    ws1 = to_words(s1)
    ws2 = to_words(s2)
    return sum(sum(word_similarity(w1, w2) for w2 in ws2) for w1 in ws1)

        
    
     




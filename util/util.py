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

def choose(items: list[T], prompt: str = __DEFAULT_PROMPT, default: Optional[T] = None) -> T:
    if default is not None and default not in items:
        items.append(default)
    if len(items) == 0:
        raise Exception("Cannot choose from no item")
    if len(items) == 1:
        return items[0]
    while (True):
        try:
            user_input = input(prompt + (f" (default: {items.index(default)} - {default})" if default else "") + ": ")
            if user_input == "":
                if default is not None:
                    return cast(T, default)
                else:
                    raise Exception
            if 0 <= (choice := int(user_input)) < len(items):
                return items[choice]
        except Exception:
            pass
        print("Invalid choice")

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
    choice = choose(items, prompt, default=default)
    print(suffix, end="")
    return choice


def yes_no_choose(prompt: str = __DEFAULT_PROMPT, prefix: str = "", suffix: str = "", default: Optional[bool] = None):
    print(prefix, end="")
    
    while True:
        choice = input(f"{prompt} ({'Y' if default == True else 'y'}/{'N' if default == False else 'n'}) ")
        if choice == "" and default is not None:
            return default
        elif choice.lower() == 'y':
            return True
        elif choice.lower() == 'n':
            return False
        print("Invalid choice")


def str_similar(s1: str, s2: str):
    
    def word_similar(w1: str, w2: str):
        if len(w1) < 3 and len(w2) < 3:
            return False
        return w1[:3] in w2 or w2[:3] in w1

    def to_words(s: str):
        return "".join(c.lower() if 96 < ord(c.lower()) < 123 else " " for c in s).split()

    ws1 = to_words(s1)
    ws2 = to_words(s2)
    print(ws1, ws2)
    for w1 in ws1:
        for w2 in ws2:
            if word_similar(w1, w2):
                print(w1, w2)
                return True
    return False 
        
    
     




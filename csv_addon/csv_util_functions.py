
def csv_column(c_name: str):
    def inner(kv: dict[str, str]):
        return kv[c_name]
    return inner

def csv_no_column(kv: dict[str, str]):
    return None

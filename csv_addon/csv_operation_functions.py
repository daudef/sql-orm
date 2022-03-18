
def csv_column(column_name: str):
    def inner(kv: dict[str, str]):
        return kv[column_name]
    return inner

csv_enum_column = csv_column

def csv_no_column(kv: dict[str, str]):
    return None

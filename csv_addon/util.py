def find_csv_delimiter(csv_filepath: str):
    delimiter = ","
    with open(file=csv_filepath) as csv_file:
        for line in csv_file:
            if line.count(";") > line.count(","):
                delimiter = ";"
            break
    return delimiter
#!/usr/bin/env python3

try:
    import code_gen_config #type: ignore
except ModuleNotFoundError:
    import sys
    sys.path[0] += "/.."

from csv_addon.profile_creator import generate_profile
from csv_addon.importer_creator import create_importer
from database import choose_database
from sys import argv
from os import system

if len(argv) != 2:
    print(f"Usage: {argv[0]} <path_to_csv>")
    exit()

csv_filepath = argv[1]
database = choose_database()

profile = generate_profile(database=database, csv_filepath=csv_filepath)
script = create_importer(database=database, profile=profile)

with open("gen/importer.py", mode="w") as f:
    f.write(script.get_script())

system(f"python3 gen/importer.py {csv_filepath}")





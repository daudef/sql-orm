from __future__ import annotations
from typing import Any
from mysql.connector import connect as sql_connect
from json import load
from os import listdir
from util.util import print_choose


def find_databases():
    databases: list[Database] = list()
    cred_dir = "credentials"
    for cred_filepath in listdir(cred_dir):
        abs_cred_filepath = f"{cred_dir}/{cred_filepath}"
        try:
            databases.append(Database.from_json_file(abs_cred_filepath))
        except Exception as e:
            raise e
    if len(databases) == 0:
        raise Exception("Cannot find any database")
    return databases

def choose_database():
    db = print_choose(find_databases(), lambda db: db.name, "Choose database: ")
    print("Choosed", db.get_full_name())
    return db

class Database:
    def __init__(self, host: str, password: str, user: str, name: str) -> None:
        self.host = host
        self.password = password
        self.user = user
        self.name = name
        self.connection = None

    def get_full_name(self):
        return f"{self.name}@{self.host}"

    @staticmethod
    def from_json_file(json_filepath: str):
        with open(file=json_filepath, mode="r") as f:
            credentials = load(f)
        return Database(host=credentials["host"], password=credentials["password"], 
                        user=credentials["user"], name=credentials["database"])

    def connect(self):
        return DatabaseConnection(self)


class DatabaseConnection:
    def __init__(self, database: Database):
        self.database = database
        self.__cnx = None

    def execute(self, request: str, params: list[Any] = list()):
        if self.__cnx is None:
            raise Exception("Cannot execute: connection is not established")
        try:
            cursor = self.__cnx.cursor()
            cursor.execute(request, params)
            results = list(cursor)
            cursor.close()
            return results
        except Exception as e:
            raise Exception(f"Request '{request} failed : {e}")

    def ask_commit(self):
        if not input(f"commit to {self.database.get_full_name()} (y/N) ") == "y":
            raise Exception("No commit")

    def __enter__(self):
        self.__cnx = sql_connect(host=self.database.host, password=self.database.password,
                                 user=self.database.user, database=self.database.name)
        return self

    def __exit__(self, exit: Any, value: Any, exc: Any):
        if self.__cnx is None:
            raise Exception("Cannot close: connection is not established")
        if (exit or value or exc) is None:
            print("No errors appened, commiting")
            self.__cnx.commit()
        else:
            print("An error happened", exit, value, exc)
        self.__cnx.close()

import mysql.connector
import mysql.connector.errorcode
import csv
from datetime import date, datetime
from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.pooling import PooledMySQLConnection
from dotenv import dotenv_values
from pathlib import Path
from typing import Union
from collections.abc import Iterable
from collections import defaultdict

AC = "ac"
SERVICE = "service"
ROLES = "roles"
USERS = "users"


def stmt_create_table() -> dict[str, str]:
    """
    Key, Value = filename, create_table_statements
    """
    hmap = defaultdict(str)
    stmt_create_table_user = """CREATE TABLE IF NOT EXISTS users (
       id INTEGER PRIMARY KEY AUTO_INCREMENT,
       name VARCHAR(32),
       email VARCHAR(255),
       password VARCHAR(32),
       gender ENUM('p', 'l'),
       photo TEXT,
       address TEXT,
       role TINYINT   
    )"""
    stmt_create_table_ac = """CREATE TABLE IF NOT EXISTS ac (
        id INTEGER PRIMARY KEY AUTO_INCREMENT,
        name TEXT,
        brand TEXT,
        pk FLOAT(23),
        price INTEGER 
    )"""
    stmt_create_table_service = """CREATE TABLE IF NOT EXISTS service (
        id INTEGER PRIMARY KEY AUTO_INCREMENT,
        technician_id INTEGER,
        client_id INTEGER,
        ac_id INTEGER,
        date DATE,
        status ENUM('finish', 'on repair', 'paid', 'unpaid')
    )"""
    stmt_create_table_roles = """CREATE TABLE IF NOT EXISTS roles (
        id INTEGER PRIMARY KEY AUTO_INCREMENT,
        name TEXT
    )"""
    hmap[USERS] = stmt_create_table_user
    hmap[ROLES] = stmt_create_table_roles
    hmap[AC] = stmt_create_table_ac
    hmap[SERVICE] = stmt_create_table_service
    return hmap


def stmt_insert_data() -> dict[str, str]:
    """
    Key, Value = filename, sql_insert_statements
    """
    hmap = defaultdict(str)
    insert_stmt_ac = """INSERT INTO ac (name, brand, pk, price) VALUES (
            %s, %s, %s, %s)
        """

    insert_stmt_service = """INSERT INTO service (technician_id, client_id, ac_id, date, status) VALUES (
            %s, %s, %s, %s, %s)
    """

    insert_stmt_roles = """INSERT INTO roles (name) VALUES (
            %s)
    """

    insert_stmt_users = """INSERT INTO users (
            name, email, password, gender, photo, address, role
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s)
    """

    hmap[USERS] = insert_stmt_users
    hmap[ROLES] = insert_stmt_roles
    hmap[AC] = insert_stmt_ac
    hmap[SERVICE] = insert_stmt_service
    return hmap


def csv_files() -> Iterable[Path]:
    csv_dir = Path("data_csv")
    if not csv_dir.exists():
        raise FileNotFoundError("csv directory not found")
    for file in csv_dir.iterdir():
        yield file


def connect_db() -> Union[PooledMySQLConnection, MySQLConnectionAbstract]:
    env = dotenv_values(".env")
    user = env.get("user")
    password = env.get("password")
    db = env.get("database")
    if user is None:
        raise ValueError("user key not found")
    if password is None:
        raise ValueError("password key not found")
    if db is None:
        raise ValueError("database key not found")
    return mysql.connector.connect(user=user, password=password, database=db)


def create_tables():
    """
    If database has no tables, invoke this function.
    """
    conn = connect_db()
    cursor = conn.cursor()
    stmt = stmt_create_table()
    for file in csv_files():
        stem = file.stem
        stmt_create = stmt[stem]
        cursor.execute(stmt_create)
    conn.close()


def field_names() -> dict[str, list[str]]:
    """
    CSV header names.
    """
    fieldnames = defaultdict(list[str])
    fieldnames[AC] = ["id", "name", "brand", "pk", "price"]
    fieldnames[USERS] = [
        "id",
        "name",
        "email",
        "password",
        "gender",
        "photo",
        "address",
        "role",
    ]
    fieldnames[SERVICE] = [
        "id",
        "technician_id",
        "client_id",
        "ac_id",
        "date",
        "status",
    ]
    fieldnames[ROLES] = ["id", "name"]
    return fieldnames


def parse_date(params: list[str | date]) -> list[str | date]:
    date_column_pos = 4
    try:
        parse_temp_date = datetime.strptime(params[date_column_pos], "%d %b %Y")
        params[date_column_pos] = date(
            parse_temp_date.year, parse_temp_date.month, parse_temp_date.day
        )
    except ValueError as err:
        print(err)
        print(params)
        print(params[date_column_pos])
        exit(1)
    return params


def insert_into_tables():
    conn = connect_db()
    cursor = conn.cursor()
    stmt = stmt_insert_data()
    fieldname_map = field_names()
    for file in csv_files():
        stem = file.stem
        with open(file, "r") as f:
            rdr = csv.DictReader(
                f,
                delimiter=",",
                fieldnames=fieldname_map[stem],
            )
            next(rdr)  # skip header
            stmt_insert = stmt[stem]
            for line in rdr:
                params = list(line.values())
                if stem == "service":
                    params = parse_date(params)
                params.pop(0)  # exclude 'id'
                try:
                    cursor.execute(stmt_insert, params)
                except mysql.connector.Error as err:
                    print(err)
                    print(stmt_insert)
                    print("file: " + stem)
                    print(params)
    conn.commit()
    conn.close()


def main():
    create_tables()
    insert_into_tables()


if __name__ == "__main__":
    main()

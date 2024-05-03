import mysql.connector
import mysql.connector.errorcode
import csv
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
        technician_id INTEGER UNIQUE,
        service_id INTEGER UNIQUE,
        ac_id INTEGER UNIQUE,
        date DATE,
        status ENUM('finish', 'on_repair', 'paid', 'unpaid')
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
    hmap = defaultdict(str)
    insert_stmt_ac = """INSERT INTO ac (id, name, brand, pk, price) VALUES (
            %(id)s, %(name)s, %(brand)s, %(pk)s, %(price))
        """

    insert_stmt_service = """INSERT INTO service (id, technician_id, client_id, service_id, ac_id, date, status) VALUES (
            %(id)s, %(technician_id)s, %(client_id)s, %(service_id)s, %(ac_id)s, %(date)s, %(status))
    """

    insert_stmt_roles = """INSERT INTO roles (id, name) VALUES (
            %(id)s, %(name)s)
    """

    insert_stmt_users = """INSERT INTO users (
            id, name, email, password, gender, photo, address, role
        ) VALUES (
            %(id)s, %(name)s, %(email)s, %(password)s, %(gender)s, %(photo)s, %(address)s, %(role)s)
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
    conn = connect_db()
    cursor = conn.cursor()
    stmt = stmt_create_table()
    for file in csv_files():
        stem = file.stem
        stmt_create = stmt[stem]
        cursor.execute(stmt_create)
    conn.close()


def insert_into_tables():
    conn = connect_db()
    cursor = conn.cursor()
    stmt = stmt_insert_data()
    for file in csv_files():
        stem = file.stem
        with open(file, "r") as file:
            rdr = csv.reader(file, delimiter=",")
        stmt_insert = stmt[stem]


def main():
    # create_tables()
    # insert_into_tables()
    ...


if __name__ == "__main__":
    main()

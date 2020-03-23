import sqlite3
from sqlite3 import Error

# Utility functions


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
        return conn
    except Error as e:
        print(e)

    return conn


def exec_query(conn, query):
    """
    Query all rows in the contracts table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute(query)

    rows = cur.fetchall()

    return rows

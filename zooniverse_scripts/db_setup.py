import argparse
import schema
import sqlite3

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
    except sqlite3.Error as e:
        print(e)

    return conn


def insert_many(conn, data, table, count):
    """
    Query all rows in the contracts table
    :param conn: the Connection object
    :return:
    """

    values = (1,) * count
    values = str(values).replace("1", "?")

    cur = conn.cursor()
    cur.executemany(f"INSERT INTO {table} VALUES {values}", data)


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


def execute_sql(conn, sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.executescript(sql)
    except sqlite3.Error as e:
        print(e)


def main():

    p = argparse.ArgumentParser(description="Input variables for table creation")
    p.add_argument(
        "-db",
        "--db_path",
        type=str,
        help="the absolute path to the database file",
        default=r"koster_lab.db", required=True
    )
    args = p.parse_args()

    sql_setup = schema.sql

    # create a database connection
    conn = create_connection(r"{:s}".format(args.db_path))

    # create tables
    if conn is not None:
        # execute sql
        execute_sql(conn, sql_setup)
    else:
        print("Error! cannot create the database connection.")


if __name__ == "__main__":
    main()

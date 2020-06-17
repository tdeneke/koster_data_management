import argparse
import schema
import sqlite3
import sys
from utils import db_utils

# Initiate the database
def main():

    p = argparse.ArgumentParser(description="Input variables for table creation")
    p.add_argument(
        "-db",
        "--db_path",
        type=str,
        help="the absolute path to the database file",
        default=r"koster_lab.db",
        required=True,
    )
    args = p.parse_args()

    sql_setup = schema.sql

    # create a database connection
    conn = db_utils.create_connection(r"{:s}".format(args.db_path))

    # create tables
    if conn is not None:
        # execute sql
        db_utils.execute_sql(conn, sql_setup)
    else:
        print("Error! cannot create the database connection.")


if __name__ == "__main__":
    main()

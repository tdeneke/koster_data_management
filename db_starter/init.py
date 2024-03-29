import os
import schema
import sqlite3
import sys
from utils import db_utils

# Initiate the database
def init_db(db_path: str):
    
    # Delete previous database versions if exists
    if os.path.exists(db_path):
        os.remove(db_path)
    
    # Get sql command for db setup
    sql_setup = schema.sql
    # create a database connection
    conn = db_utils.create_connection(r"{:s}".format(db_path))

    # create tables
    if conn is not None:
        # execute sql
        db_utils.execute_sql(conn, sql_setup)
        return "Database creation success"
    else:
        return "Database creation failure"

import os, cv2, csv, json, sys, io
import operator, argparse, requests
import pandas as pd
import sqlite3
from datetime import datetime
import utils.db_utils as db_utils
from static import get_length

def add_new_movies(movies_file_id, db_path, movies_path):

    # Download the csv with movies information from the google drive
    movies_df = db_utils.download_csv_from_google_drive(movies_file_id)

    # Include server's path of the movie files
    movies_df["Fpath"] = movies_path + "/" + movies_df["FilenameCurrent"] + ".mov"

    # Standarise the filename
    movies_df["FilenameCurrent"] = movies_df["FilenameCurrent"].str.normalize("NFD")
    
    # Update movies table
    conn = db_utils.create_connection(db_path)

    # Reference with sites table
    sites_df = pd.read_sql_query("SELECT id, name FROM sites", conn)
    sites_df = sites_df.rename(columns={"id": "Site_id"})

    movies_df = pd.merge(
        movies_df, sites_df, how="left", left_on="SiteDescription", right_on="name"
    )

    # Calculate the fps and length of the original movies
    movies_df[["fps", "duration"]] = pd.DataFrame(movies_df["Fpath"].apply(get_length, 1).tolist(), columns=["fps", "duration"])
    
    # Select only those fields of interest
    movies_db = movies_df[
        ["FilenameCurrent", "DateFull", "fps", "duration", "Author", "Site_id", "Fpath"]
    ]

    # Add values to movies table
    db_utils.add_to_table(
        db_path, "movies", [(None,) + tuple(i) for i in movies_db.values], 8
    )
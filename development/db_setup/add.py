import os, cv2, csv, json, sys, io, re
import operator, argparse, requests
import pandas as pd
import sqlite3
from datetime import datetime
import utils.db_utils as db_utils

def get_length(video_file):
    final_fn = video_file if os.path.isfile(video_file) else db_utils.unswedify(video_file)
    if os.path.isfile(final_fn):
        cap = cv2.VideoCapture(final_fn)
        fps = cap.get(cv2.CAP_PROP_FPS)     
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        length = frame_count/fps
    else:
        length, fps = None, None
    return fps, length

def add_new_movies(movies_file_id, db_path, movies_path):

    # Download the csv with movies information from the google drive
    movies_df = db_utils.download_csv_from_google_drive(movies_file_id)

    # Include server's path of the movie files
    movies_df["Fpath"] = movies_path + "/" + movies_df["FilenameCurrent"]

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

def add_species(species_file_id, db_path):

    # Download the csv with species information from the google drive
    species_df = db_utils.download_csv_from_google_drive(species_file_id)

    # Create connection to db
    conn = db_utils.create_connection(db_path)

    # Retrieve the id and label from the species table
    uploaded_species = pd.read_sql_query("SELECT id, label FROM species", conn)
    uploaded_species = uploaded_species.rename(columns={"id": "species_id"})

    species_df["Fixed_Name"] = species_df["Name"].apply(lambda x: re.sub(r"[()\s]", "", x).lower(), 1)
    species_df = species_df[~species_df["Fixed_Name"].isin([re.sub(r"[()\s]", "", i).lower() for i in uploaded_species["label"].unique()])]

    # Add values to species table
    db_utils.add_to_table(
        db_path, "species", [(None,) + tuple([i]) for i in species_df["Name"].values], 2
    )


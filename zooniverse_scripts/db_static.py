import os, csv, json, sys
import operator, argparse
import requests
import io
import pandas as pd
import sqlite3
from datetime import datetime
from db_setup import *
from zooniverse_setup import *


def download_csv_from_google_drive(id):

    # Download the csv files stored in Google Drive with initial information about
    # the movies and the species

    URL = "https://docs.google.com/uc?export=download"

    session = requests.Session()

    response = session.get(URL, params={"id": id}, stream=True)
    token = get_confirm_token(response)

    if token:
        params = {"id": id, "confirm": token}
        response = session.get(URL, params=params, stream=True)

    return response


def get_confirm_token(response):
    for key, value in response.cookies.items():
        if key.startswith("download_warning"):
            return value

    return None


def get_site_id(row):

    # Currently we discard sites that have no lat or lon coordinates, since site descriptions are not unique
    # it becomes difficult to match this information otherwise

    try:
        site_id = retrieve_query(
            conn,
            f"SELECT id FROM sites WHERE coord_lat=={row['CentroidLat']} AND coord_lon=={row['CentroidLong']}",
        )[0][0]
    except:
        site_id = None
    return site_id


def add_movies(movies_file_id, db_path):

    # Download the csv with movies information from the google drive
    movies_csv_resp = download_csv_from_google_drive(movies_file_id)
    movies_df = pd.read_csv(io.StringIO(movies_csv_resp.content.decode("utf-8")))

    # Include server's path of the movie files
    movies_df["Fpath"] = movies_df['FilenameCurrent']+".mov"
    
    # Set up sites information
    sites_db = movies_df[["SiteDecription", "CentroidLat", "CentroidLong"]].drop_duplicates("SiteDecription")

    # Update sites table
    conn = create_connection(db_path)

    try:
        # An additional None is added at the front to account for the autoincrement id column
        insert_many(
            conn, [(None,) + tuple(i) + (None,) for i in sites_db.values], "sites", 5
        )
    except sqlite3.Error as e:
        print(e)

    conn.commit()

    # Update movies table
    conn = create_connection(db_path)
    
    # Reference with sites table
    sites_df = pd.read_sql_query("SELECT id, name FROM sites", conn)
    sites_df = sites_df.rename(columns={"id": "Site_id"})
    
    movies_df = pd.merge(movies_df,
                         sites_df,
                         how='left',
                         left_on= 'SiteDecription',
                         right_on= 'name')
    
    # Select only those fields of interest
    movies_db = movies_df[
        ["FilenameCurrent", "DateFull", "Total_time", "Author", "Site_id", "Fpath"]
    ]
    
    try:
        insert_many(
            conn, [(None,) + tuple(i) for i in movies_db.values], "movies", 7
        )
    except sqlite3.Error as e:
        print(e)

    conn.commit()

    print("Updated sites and movies")


def add_species(species_file_id, db_path):

    # Download the csv with species information from the google drive
    species_csv_resp = download_csv_from_google_drive(species_file_id)
    species_df = pd.read_csv(io.StringIO(species_csv_resp.content.decode("utf-8")))

    # Update movies table
    conn = create_connection(db_path)

    try:
        # An additional None is added at the front to account for the autoincrement id column
        insert_many(
            conn,
            [(None,) + tuple([i]) for i in species_df["Name"].values],
            "species",
            2,
        )
    except sqlite3.Error as e:
        print(e)

    conn.commit()

    print("Updated species")


def main():
    "Handles argument parsing and launches the correct function."
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--species_file_id",
        "-sp",
        help="Google drive id of species csv file",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--movies_file_id",
        "-mov",
        help="Google drive id of movies csv file",
        type=str,
        required=True,
    )
    parser.add_argument(
        "-db",
        "--db_path",
        type=str,
        help="the absolute path to the database file",
        default=r"koster_lab.db",
        required=True,
    )

    args = parser.parse_args()

    add_movies(args.movies_file_id, args.db_path)
    add_species(args.species_file_id, args.db_path)


if __name__ == "__main__":
    main()

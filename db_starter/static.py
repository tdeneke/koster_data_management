import os, cv2, csv, json, sys, io
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


def add_sites(sites_csv, db_path):

    # TODO check if sites_csv exists otherwise download sites from Google Drive
    
    # Load the csv with sites information
    sites_df = pd.read_csv(sites_csv)
    
    
    # Select relevant fields
    sites_df = sites_df[
        ["koster_site_id", "siteName", "decimalLatitude", "decimalLongitude", "geodeticDatum", "countryCode"]
    ]
    
    ### TODO add a roadblock to prevent empty lat/long/datum/countrycode

    # Add values to sites table
    db_utils.add_to_table(
        db_path, "sites", [tuple(i) for i in sites_df.values], 6
    )

    
def add_movies(movies_csv, movies_path, db_path):

    # TODO check if movies_csv exists otherwise download sites from Google Drive
    
    # Load the csv with movies information
    movies_df = pd.read_csv(movies_csv)

    # Include server's path to the movie files
    movies_df["Fpath"] = movies_path + "/" + movies_df["filename"]
    
    # Check that videos can be mapped
    movies_df['exists'] = movies_df['Fpath'].map(os.path.isfile)
    
    # Standarise the filename
    movies_df["filename"] = movies_df["filename"].str.normalize("NFD")
    
    # Ensure all videos have fps and duration information
    if movies_df["fps"].isna().any()|movies_df["duration"].isna().any():
            
            # Select only those movies with missing fps or duration
            missing_fps_movies = movies_df[movies_df[["fps", "duration"]].isna().any(axis=1)]
            
            # Prevent missing fps and duration information
            if len(missing_fps_movies[~missing_fps_movies.exists]) > 0:
                print(
                    f"There are {len(missing_fps_movies) - missing_fps_movies.exists.sum()} out of {len(missing_fps_movies)} movies missing from the server without fps and/or duration information. The movie filenames are {missing_fps_movies[~missing_fps_movies.exists].filename.tolist()}"
                )
                
                return
            
            else: 
                # Calculate the fps and length of the original movies
                movies_df[["fps", "duration"]] = pd.DataFrame(missing_fps_movies["Fpath"].apply(get_length, 1).tolist(), columns=["fps", "duration"])
            
                # TODO update the local movies.csv file with the new fps and duration information and upload to Google Drive csv safe copy
                
                print(
                    f" The fps and duration of {len(missing_fps_movies)} movies have been succesfully added"
                )
                
                
    
    
    # Ensure date is ISO 8601:2004(E) compatible with Darwin Data standards
    #try:
    #    date.fromisoformat(movies_df['eventDate'])
    #except ValueError:
    #    print("Invalid eventDate column")

    # Connect to koster database
    conn = db_utils.create_connection(db_path)
    
    # TODO add roadblock to check all movies have site references
    # Reference movies with their respective sites
    sites_df = pd.read_sql_query("SELECT id, siteName FROM sites", conn)
    sites_df = sites_df.rename(columns={"id": "Site_id"})

    movies_df = pd.merge(
        movies_df, sites_df, how="left", on="siteName"
    )

    
    # Select only those fields of interest
    movies_db = movies_df[
        ["koster_movie_id", "filename", "created_on", "fps", "duration", "Author", "Site_id", "Fpath"]
    ]

    # Add values to movies table
    db_utils.add_to_table(
        db_path, "movies", [tuple(i) for i in movies_db.values], 8
    )


def add_species(species_csv, db_path):

    # TODO check if species_csv exists otherwise download sites from Google Drive
    
    # Load the csv with species information
    species_df = pd.read_csv(species_csv)
    
    # Select relevant fields
    species_df = species_df[
        ["koster_species_id", "commonName", "scientificName", "taxonRank", "kingdom"]
    ]
    
    # Add values to species table
    db_utils.add_to_table(
        db_path, "species", [tuple(i) for i in species_df.values], 5
    )


def main():
    "Handles argument parsing and launches the correct function."
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sites_csv",
        "-sit",
        help="Filepath of the csv file with info about the sites",
        type=str,
        default=r"../db_starter/db_csv_info/sites_koster.csv",
    )
    parser.add_argument(
        "--movies_csv",
        "-mov",
        help="Filepath of the csv file with info about the movies",
        type=str,
        default=r"../db_starter/db_csv_info/movies_koster.csv",
    )
    parser.add_argument(
        "--species_csv",
        "-sp",
        help="Filepath of the csv file with info about the species",
        type=str,
        default=r"../db_starter/db_csv_info/species_koster.csv",
    )
    parser.add_argument(
        "-mp",
        "--movies_path",
        type=str,
        help="Absolute path to the movie files",
        default=r"/uploads",
    )
    parser.add_argument(
        "-db",
        "--db_path",
        type=str,
        help="the absolute path to the database file",
        default=r"koster_lab.db",
    )

    args = parser.parse_args()
    
    #TODO add Google Drive template and provide option for koster csv
    
    add_sites(args.sites_csv, args.db_path)
    add_movies(args.movies_csv, args.movies_path, args.db_path)
    add_species(args.species_csv, args.db_path)


if __name__ == "__main__":
    main()

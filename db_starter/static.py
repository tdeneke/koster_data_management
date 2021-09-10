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


def get_movie_parameters(df, movies_csv):
    
    # Specify the parameters of the movies
    parameters = ["fps", "duration", "survey_start", "survey_end"]
    
    for parameter in parameters:
    
        # Check if the parameter is missing from any movie
        if df[parameter].isna().any():
            
            # Select only those movies with the missing parameter
            miss_par_df = df[df[parameter].isna()]
            
            if parameter in ["fps","duration"]:
                
                # Prevent missing parameters from movies that don't exists
                if len(miss_par_df[~miss_par_df.exists]) > 0:
                    print(
                        f"There are {len(miss_par_df) - miss_par_df.exists.sum()} out of {len(miss_par_df)} movies missing from the server without {parameter} information. The movies are {miss_par_df[~miss_par_df.exists].filename.tolist()}"
                    )

                    return
                
                # Set the fps and duration of each movide
                df.loc[df["fps"].isna()|df["duration"].isna(), "fps": "duration"] = pd.DataFrame(df["Fpath"].apply(get_length, 1).tolist(), columns=["fps", "duration"])
            
            if parameter == "survey_start":
                # Set the start of each movie to 0
                df.loc[df["survey_start"].isna(),"survey_start"] = 0

            if parameter == "survey_end":
                # Set the end of each movie to the duration of the movie
                df.loc[df["survey_end"].isna(),"survey_end"] = df["duration"]

            # Update the local movies.csv file with the new fps and duration information
            df.drop(["Fpath","exists"], axis=1).to_csv(movies_csv,index=False)

            print(
                f" The {parameter} information of {len(miss_par_df)} movies have been succesfully added to the local csv file"
            )

        # Prevent ending survey times longer than actual movies
        if parameter is ["survey_end"] and (df["survey_end"] > df["duration"]).any():
            print(
                f"The survey_end times of {df[~df.exists].filename.tolist()} are longer than the actual movies"
            )

            return

    return df


def add_sites(sites_csv, db_path):

    # Load the csv with sites information
    sites_df = pd.read_csv(sites_csv)
    
    
    # Select relevant fields
    sites_df = sites_df[
        ["site_id", "siteName", "decimalLatitude", "decimalLongitude", "geodeticDatum", "countryCode"]
    ]
    
    # Roadblock to prevent empty lat/long/datum/countrycode
    db_utils.test_table(
        sites_df, "sites", sites_df.columns
    )

    # Add values to sites table
    db_utils.add_to_table(
        db_path, "sites", [tuple(i) for i in sites_df.values], 6
    )

    
def add_movies(movies_csv, movies_path, db_path):

    # Load the csv with movies information
    movies_df = pd.read_csv(movies_csv)

    # Include server's path to the movie files
    movies_df["Fpath"] = movies_path + "/" + movies_df["filename"]
    
    # Check that videos can be mapped
    movies_df['exists'] = movies_df['Fpath'].map(os.path.isfile)
    
    # Standarise the filename
    movies_df["filename"] = movies_df["filename"].str.normalize("NFD")
    
    # Ensure all videos have fps, duration, starting and ending time of the survey
    movies_df = get_movie_parameters(movies_df, movies_csv)
    
    # Ensure date is ISO 8601:2004(E) compatible with Darwin Data standards
    #try:
    #    date.fromisoformat(movies_df['eventDate'])
    #except ValueError:
    #    print("Invalid eventDate column")

    # Connect to database
    conn = db_utils.create_connection(db_path)
    
    # Reference movies with their respective sites
    sites_df = pd.read_sql_query("SELECT id, siteName FROM sites", conn)
    sites_df = sites_df.rename(columns={"id": "Site_id"})


    
    movies_df = pd.merge(
        movies_df, sites_df, how="left", on="siteName"
    )
    

    # Select only those fields of interest
    movies_db = movies_df[
        ["movie_id", "filename", "created_on", "fps", "duration", "survey_start", "survey_end", "Author", "Site_id", "Fpath"]
    ]
    
    # Roadblock to prevent empty information
    db_utils.test_table(
        movies_db, "movies", movies_db.columns
    )
    
    # Add values to movies table
    db_utils.add_to_table(
        db_path, "movies", [tuple(i) for i in movies_db.values], 10
    )


def add_species(species_csv, db_path):

    # Load the csv with species information
    species_df = pd.read_csv(species_csv)
    
    # Select relevant fields
    species_df = species_df[
        ["species_id", "commonName", "scientificName", "taxonRank", "kingdom"]
    ]
    
    # Roadblock to prevent empty information
    db_utils.test_table(
        species_df, "species", species_df.columns
    )
    
    # Add values to species table
    db_utils.add_to_table(
        db_path, "species", [tuple(i) for i in species_df.values], 5
    )
    

def static_setup(sites_csv: str,
                 movies_csv: str,
                 species_csv: str,
                 movies_path: str,
                 db_path: str):   
    
    add_sites(sites_csv, db_path)
    add_movies(movies_csv, movies_path, db_path)
    add_species(species_csv, db_path)

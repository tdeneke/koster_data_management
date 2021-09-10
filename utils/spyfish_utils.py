#spyfish utils
import sqlite3
import pandas as pd

import utils.db_utils as db_utils

    
def process_spyfish_subjects(subjects, db_path):
    
    # Merge "#Subject_type" and "Subject_type" columns to "subject_type"
    subjects['subject_type'] = subjects['Subject_type'].fillna(subjects['#Subject_type'], inplace=True)
    
    # Rename columns to match the db format
    subjects = subjects.rename(
        columns={
            "#VideoFilename": "filename",
            "upl_seconds": "clip_start_time",
            "#frame_number": "frame_number"
        }
    )
    
    # Calculate the clip_end_time
    subjects["clip_end_time"] = subjects["clip_start_time"] + subjects["#clip_length"] 
    
    # Create connection to db
    conn = db_utils.create_connection(db_path)
    
    ##### Match 'ScientificName' to species id and save as column "frame_exp_sp_id" 
    # Query id and sci. names from the species table
    species_df = pd.read_sql_query("SELECT id, scientificName FROM species", conn)
    
    # Rename columns to match subject df 
    species_df = species_df.rename(
        columns={
            "id": "frame_exp_sp_id",
            "scientificName": "ScientificName"
        }
    )
    
    # Reference the expected species on the uploaded subjects
    subjects = pd.merge(subjects, species_df, how="left", on="ScientificName")

    ##### Match "#VideoFilename" to name from movies sql and get movie_id to save it as "movie_id"
    # Query id and filenames from the movies table
    movies_df = pd.read_sql_query("SELECT id, filename FROM movies", conn)
    
    # Rename columns to match subject df 
    movies_df = movies_df.rename(
        columns={
            "id": "movie_id"
        }
    )
    
    # Reference the movienames with the id movies table
    subjects = pd.merge(subjects, movies_df, how="left", on="filename")
    
    return subjects

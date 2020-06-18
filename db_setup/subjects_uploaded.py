import io, os, json, csv
import sqlite3
import requests, argparse
import pandas as pd
import numpy as np

from datetime import datetime
from panoptes_client import Project, Panoptes
import utils.db_utils as db_utils
from utils.zooniverse_utils import auth_session

# Function to extract the metadata from subjects
def extract_metadata(subj_df):

    # Reset index of df
    subj_df = subj_df.reset_index(drop=True).reset_index()

    # Flatten the metadata information
    meta_df = pd.json_normalize(subj_df.metadata.apply(json.loads))

    # Drop metadata and index columns from original df
    subj_df = subj_df.drop(columns=["metadata", "index",])

    return subj_df, meta_df


# Function to get the movie_ids based on movie filenames
def get_movies_id(df, db_path):

    # Create connection to db
    conn = db_utils.create_connection(db_path)

    # Query id and filenames from the movies table
    movies_df = pd.read_sql_query("SELECT id, filename FROM movies", conn)
    movies_df = movies_df.rename(
        columns={"id": "movie_id", "filename": "movie_filename"}
    )
    
    # Reference the manually uploaded subjects with the movies table
    df = pd.merge(df, movies_df, how="left", on="movie_filename")

    # Drop the movie_filename column
    df = df.drop(columns=["movie_filename",])

    return df


# Function to process the metadata of clips that were uploaded manually
def process_manual_clips(meta_df, db_path):

    # Select the filename of the clips and remove extension type
    clip_filenames = meta_df["filename"].str.replace(".mp4", "")

    # Get the starting time of clips in relation to the original movie
    # split the filename and select the last section
    meta_df["clip_start_time"] = (
        clip_filenames.str.rsplit("_", 1).str[-1]
    )

    # Extract the filename of the original movie
    meta_df["movie_filename"] = meta_df.apply(
        lambda x: x["filename"].replace("_" + x["clip_start_time"], "").replace(".mp4", ""), axis=1
    )
  
    # Get the end time of clips in relation to the original movie
    meta_df["clip_start_time"] = pd.to_numeric(
        meta_df["clip_start_time"], downcast="signed"
    )
    meta_df["clip_end_time"] = meta_df["clip_start_time"] + 10

    # Select only relevant columns
    meta_df = meta_df[
        ["filename", "movie_filename", "clip_start_time", "clip_end_time"]
    ]

    # Include movie_ids to the metadata
    meta_df = get_movies_id(meta_df, db_path)
    
    if meta_df.movie_id.isnull().values.any():
        print(
            "There are clip subjects that don't have movie_id"
        )
        raise
        
    return meta_df


def main():

    "Handles argument parsing and launches the correct function."
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--user", "-u", help="Zooniverse username", type=str, required=True
    )
    parser.add_argument(
        "--password", "-p", help="Zooniverse password", type=str, required=True
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

    # Connect to the Zooniverse project
    project = auth_session(args.user, args.password)

    # Get info of subjects uploaded to the project
    export = project.get_export("subjects")

    # Save the subjects info as pandas data frame
    subjects_df = pd.read_csv(
        io.StringIO(export.content.decode("utf-8")),
        usecols=[
            "subject_id",
            "metadata",
            "created_at",
            "workflow_id",
            "subject_set_id",
            "classifications_count",
            "retired_at",
            "retirement_reason",
        ],
    )

    ### Update subjects uploaded automatically ###

    # Specify the date when the metadata of subjects uploaded matches schema.py
    first_auto_upload = "2020-05-29 00:00:00 UTC"

    # Select automatically uploaded frames
    auto_subjects_df = subjects_df[subjects_df["created_at"] > first_auto_upload]

    # Extract metadata from automatically uploaded frames
    auto_subjects_df, auto_subjects_meta = extract_metadata(auto_subjects_df)

    # Combine metadata info with the subjects df
    auto_subjects_df = pd.concat([auto_subjects_df, auto_subjects_meta], axis=1)

    # TODO Check movie_ids of automatically uploaded subjects are correct

    ### Update subjects uploaded manually ###

    # Specify the starting date when clips were manually uploaded
    first_manual_upload = "2019-11-17 00:00:00 UTC"

    # Select clips uploaded manually
    man_clips_df = (
        subjects_df[
            (subjects_df["metadata"].str.contains(".mp4"))
            & (
                subjects_df["created_at"].between(
                    first_manual_upload, first_auto_upload
                )
            )
        ]
        .reset_index(drop=True)
        .reset_index()
    )

    # Specify the type of subject
    man_clips_df["subject_type"] = "clip"

    # Extract metadata from manually uploaded clips
    man_clips_df, man_clips_meta = extract_metadata(man_clips_df)

    # Process the metadata of manually uploaded clips
    man_clips_meta = process_manual_clips(man_clips_meta, args.db_path)

    # Combine metadata info with the subjects df
    man_clips_df = pd.concat([man_clips_df, man_clips_meta], axis=1)

    ### Update subjects table ###

    # Combine all uploaded subjects
    subjects = pd.merge(man_clips_df, auto_subjects_df, how="outer")

    # Set subject_id information as id
    subjects = subjects.rename(columns={"subject_id": "id",})

    # Set the columns in the right order
    subjects = subjects[
        [
            "id",
            "subject_type",
            "filename",
            "clip_start_time",
            "clip_end_time",
            "frame_exp_sp_id",
            "frame_number",
            "workflow_id",
            "subject_set_id",
            "classifications_count",
            "retired_at",
            "retirement_reason",
            "created_at",
            "movie_id",
        ]
    ]

    # Test table validity
    db_utils.test_table(subjects, "subjects", keys=["movie_id"])

    # Add values to subjects
    db_utils.add_to_table(
        args.db_path, "subjects", [tuple(i) for i in subjects.values], 14
    )


if __name__ == "__main__":
    main()

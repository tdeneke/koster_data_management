import io, os, json, csv
import sqlite3, db_utils
import requests, argparse
import pandas as pd
import numpy as np
from datetime import datetime
from panoptes_client import Project, Panoptes
from zooniverse_setup import auth_session

# Function to extract the metadata from subjects df
def extract_metadata(subj_df):

    # Reset index of df
    subj_df = subj_df.reset_index(drop=True).reset_index()

    # Flatten the metadata information
    meta_df = pd.json_normalize(subj_df.metadata.apply(json.loads))

    # Drop metadata and index columns from original df
    subj_df = subj_df.drop(columns=["metadata", "index",])

    return subj_df, meta_df


# Function to get the movie_ids based on the movie filenames
def get_movies_id(df, db_path):

    # Create connection to db
    conn = db_utils.create_connection(db_path)

    # Query id and filenames from the movies table
    movies_df = pd.read_sql_query("SELECT id, filename FROM movies", conn)
    movies_df = movies_df.rename(
        columns={"id": "movie_id", "filename": "movie_filename"}
    )

    # Unswedify movie filenames
    movies_df["movie_filename"] = movies_df["movie_filename"].str.normalize("NFD")
    df["movie_filename"] = df["movie_filename"].str.normalize("NFD")

    # Reference the manually uploaded subjects with the movies table
    df = pd.merge(df, movies_df, how="left", on="movie_filename")

    # Drop the movie_filename column
    df = df.drop(columns=["movie_filename",])

    return df


# Function to process the metadata of manually uploaded clips
def process_manual_clips(meta_df, db_path):

    # Select the filename of the clips
    clip_filenames = meta_df["filename"]

    # Get the starting time of clips in relation to the original movie
    # split the filename, select the last section, and remove the extension type
    meta_df["clip_start_time"] = (
        clip_filenames.str.rsplit("_", 1).str[-1].str.replace(".mp4", "")
    )

    # Extract the filename of the original movie
    meta_df["movie_filename_ext"] = meta_df.apply(
        lambda x: x["filename"].replace("_" + x["clip_start_time"], ""), axis=1
    )

    # Remove the extension of the filename of the original movie
    meta_df["movie_filename"] = meta_df["movie_filename_ext"].str.replace(".mp4", "")

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

    # Add a column that diferentiates clips from frames
    conditions = [
        (subjects_df["metadata"].str.contains(".mp4")),
        (subjects_df["metadata"].str.contains("frame_")),
    ]
    choices = ["clip", "frame"]
    subjects_df["subject_type"] = np.select(conditions, choices, default="unknown")

    ### Clip subjects ###

    # Select clip subjects
    clips_df = subjects_df[subjects_df["subject_type"].str.contains("clip")]

    # Specify the dates when clips were manually uploaded
    first_manual_clips = "2019-11-17 00:00:00 UTC"
    last_manual_clips = "2020-05-01 00:00:00 UTC"

    # Select manually uploaded clips
    man_clips_df = clips_df[
        clips_df["created_at"].between(first_manual_clips, last_manual_clips)
    ]

    # Extract metadata from manually uploaded clips
    man_clips_df, man_clips_meta = extract_metadata(man_clips_df)

    # Process the metadata of manually uploaded clips
    man_clips_meta = process_manual_clips(man_clips_meta, args.db_path)

    # Combine metadata info with the subjects df
    man_clips_df = pd.concat([man_clips_df, man_clips_meta], axis=1)

    ### Frame subjects ###

    # Select frame subjects
    frames_df = subjects_df[subjects_df["subject_type"].str.contains("frame")]

    # Specify the date range when frames were manually uploaded
    first_manual_frames = "2019-11-17 00:00:00 UTC"
    last_manual_frames = "2020-05-25 00:00:00 UTC"

    # Select manually uploaded frames
    man_frames_df = frames_df[
        frames_df["created_at"].between(first_manual_frames, last_manual_frames)
    ]

    # Extract metadata from manually uploaded frames
    man_frames_df, man_frames_meta = extract_metadata(man_frames_df)

    # Rename columns to match the subjects table
    man_frames_meta = man_frames_meta.rename(columns={"movie_frame": "frame_number"})

    # Combine metadata info with the subjects df
    man_frames_df = pd.concat([man_frames_df, man_frames_meta], axis=1)

    # Select automatically uploaded frames
    # auto_frames_df = frames_df[frames_df["created_at"]>last_manual_frames]

    # Extract metadata from automatically uploaded frames
    # auto_frames_df, auto_frames_meta = extract_metadata(auto_frames_df)

    # Combine metadata info with the subjects df
    # auto_frames_df = pd.concat([auto_frames_df, auto_frames_meta], axis=1)

    # TODO Check movie_ids of automatically uploaded subjects are correct

    ### Update subjects table ###

    # Combine all uploaded subjects
    subjects = pd.merge(man_clips_df, man_frames_df, how="outer")

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

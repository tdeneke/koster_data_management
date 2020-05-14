import io, os, json, csv
import sqlite3, db_utils
import requests, argparse
import pandas as pd
import numpy as np
from datetime import datetime
from panoptes_client import Project, Panoptes
from zooniverse_setup import auth_session


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
    (subjects_df["metadata"].str.contains(".jpg"))
    ]
    choices = ['clip', 'frame']
    subjects_df["subject_type"] = np.select(conditions, choices, default='unknown')
    
    # Specify dates when finalised clips and frames started to get uploaded
    first_clips_date = "2019-11-17 00:00:00 UTC"
    first_frames_date = "2020-06-12 00:00:00 UTC"
    
    # Select clip subjects
    clips_df = subjects_df[
        (subjects_df.subject_type == "clip") & (first_clips_date <= subjects_df.created_at)
    ].reset_index(drop=True).reset_index()

    # Flatten the metadata information
    clips_metadata = pd.json_normalize(clips_df.metadata.apply(json.loads))

    # Select the filename of the clips
    clip_filenames = clips_metadata["filename"]

    # Get the starting time of clips in relation to the original movie
    # split the filename, select the last section, and remove the extension type
    clips_metadata["clip_start_time"] = (
        clip_filenames.str.rsplit("_", 1).str[-1].str.replace(".mp4", "")
    )

    # Extract the filename of the original movie
    clips_metadata["movie_filename_ext"] = clips_metadata.apply(
        lambda x: x["filename"].replace("_" + x["clip_start_time"], ""), axis=1
    )

    # Remove the extension of the filename of the original movie
    clips_metadata["movie_filename"] = clips_metadata["movie_filename_ext"].str.replace(
        ".mp4", ""
    )

    # Get the end time of clips in relation to the original movie
    clips_metadata["clip_start_time"] = pd.to_numeric(
        clips_metadata["clip_start_time"], downcast="signed"
    )
    clips_metadata["clip_end_time"] = clips_metadata["clip_start_time"] + 10

    # Select only relevant columns
    clips_metadata = clips_metadata[
        ["filename", "movie_filename", "clip_start_time", "clip_end_time"]
    ]

    # Create connection to db
    conn = db_utils.create_connection(args.db_path)

    # Query id and filenames from the movies table
    movies_df = pd.read_sql_query("SELECT id, filename FROM movies", conn)
    movies_df = movies_df.rename(
        columns={"id": "movie_id", "filename": "movie_filename"}
    )

    # Deal with special characters
    movies_df["movie_filename"] = movies_df["movie_filename"].str.normalize("NFD")
    clips_metadata["movie_filename"] = clips_metadata["movie_filename"].str.normalize("NFD")

    # Reference the clips with the movies table
    clips_metadata = pd.merge(clips_metadata, movies_df, how="left", on="movie_filename")

    # Combine the metadata information
    clips_df = pd.concat([clips_df, clips_metadata], axis=1)
    
    ### Create empty columns non relevant for clips###
    clips_df["frame_exp_sp_id"], clips_df["frame_number"] = None, None
    
    # Drop unnecessary columns
    clips_df = clips_df.drop(
        columns=[
            "index",
            "metadata",
            "movie_filename",
        ]
    ) 
    
    # Select frames subjects
    frames_df = subjects_df[
        (subjects_df.subject_type == "frame") & (first_frames_date >= subjects_df.created_at)
    ].reset_index()
    
    if len(frames_df) > 0:
        # Flatten the metadata information
        frames_metadata = pd.json_normalize(frames_df.metadata.apply(json.loads))
        
        # Combine the metadata information
        frames_df = pd.concat([frames_df, frames_metadata], axis=1)
        
        ### Create empty columns non relevant for frames###
        frames_df["clip_start_time"], frames_df["clip_end_time"] = None, None
        
        # Combine the frame and clip subjects
        subjects = clips_df.append(frames_df)
        
    else:
        # Combine the frame and clip subjects
        subjects = clips_df
        
    # Set subject_id information as id
    subjects = subjects.rename(
        columns={
            "subject_id": "id",
        }
    )

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

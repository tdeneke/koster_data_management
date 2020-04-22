import io, os, json, csv
import sqlite3
import requests, argparse
import pandas as pd
import numpy as np
from datetime import datetime
from panoptes_client import Project, Panoptes
from db_setup import *
from zooniverse_setup import *


def get_id(conn, row):

    try:
        filename, ext = os.path.splitext(row["movie_filename"])
        gid = retrieve_query(
            conn, f"SELECT id FROM movies WHERE filename=='{filename}'"
        )[0][0]
    except:
        gid = None
    return gid


def test_table(table, db_table_name, keys=['id']):
    try:
        # check that there are no id columns with a NULL value, which means that they were not matched
        assert len(table[table[keys].isnull().any(axis=1)]) == 0
    except AssertionError:
        print(
            f"The table {db_table_name} has invalid entries, please ensure that all columns are non-zero"
        )


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

    project = auth_session(args.user, args.password)

    # Specify the last and first dates when subjects were manually uploaded
    last_date = "2020-02-03 20:30:00 UTC"
    first_date = "2019-11-17 00:00:00 UTC"

    # get the export subjects
    export = project.get_export("subjects")

    # save the response as pandas data frame
    rawdata = pd.read_csv(
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

    # Filter manually uploaded subjects
    man_data = rawdata[
        (last_date >= rawdata.created_at) & (first_date <= rawdata.created_at)
    ]

    # filter clip subjects and reset index
    man_data = (
        man_data[man_data["metadata"].str.contains(".mp4")]
        .reset_index(drop=True)
        .reset_index()
    )

    # Flatten the metadata information
    flat_metadata = pd.json_normalize(man_data.metadata.apply(json.loads))

    # Select the filename of the clips
    clip_filenames = flat_metadata["filename"]

    # Get the starting time of clips in relation to the original movie
    # split the filename, select the last section, and remove the extension type
    flat_metadata["start_time"] = (
        clip_filenames.str.rsplit("_", 1).str[-1].str.replace(".mp4", "")
    )

    # Extract the filename of the original movie
    flat_metadata["movie_filename_ext"] = flat_metadata.apply(
        lambda x: x["filename"].replace("_" + x["start_time"], ""), axis=1
    )

    # Remove the extension of the filename of the original movie
    flat_metadata["movie_filename"] = flat_metadata["movie_filename_ext"].str.replace(
        ".mp4", ""
    )

    # Get the end time of clips in relation to the original movie
    flat_metadata["start_time"] = pd.to_numeric(
        flat_metadata["start_time"], downcast="signed"
    )
    flat_metadata["end_time"] = flat_metadata["start_time"] + 10

    # select only relevant columns
    flat_metadata = flat_metadata[
        ["filename", "movie_filename", "start_time", "end_time"]
    ]

    # create connection to db
    conn = create_connection(args.db_path)

    # Query id and filenames from the movies table
    movies_df = pd.read_sql_query("SELECT id, filename FROM movies", conn)
    movies_df = movies_df.rename(
        columns={"id": "movie_id", "filename": "movie_filename"}
    )

    # Deal with special characters
    movies_df["movie_filename"] = movies_df["movie_filename"].str.normalize("NFD")
    flat_metadata["movie_filename"] = flat_metadata["movie_filename"].str.normalize(
        "NFD"
    )

    # Reference with movies table
    flat_metadata = pd.merge(flat_metadata, movies_df, how="left", on="movie_filename")

    # Drop metadata column and define clip creation date as time uploaded to Zooniverse
    man_data = man_data.drop(columns="metadata")

    # Combine the information
    comb_data = pd.concat([man_data, flat_metadata], axis=1)

    # Select information to include in the clips table
    clips = comb_data.drop(
        columns=[
            "index",
            "subject_id",
            "movie_filename",
            "workflow_id",
            "subject_set_id",
            "classifications_count",
            "retired_at",
            "retirement_reason",
        ]
    ).rename(columns={"created_at": "clipped_date"})


    # test table validity 
    test_table(clips, "clips", keys=['movie_id'])

    # Update the clips table
    try:
        insert_many(conn, [(None,) + tuple(i) for i in clips.values], "clips", 6)
    except sqlite3.Error as e:
        print(e)

    # Combine the info to include in the subjects table
    subjects = comb_data.rename(
        columns={
            "created_at": "zoo_upload_date",
            "retirement_reason": "retirement_criteria",
            "subject_id": "id",
        }
    )

    subjects = subjects[
        [
            "id",
            "workflow_id",
            "subject_set_id",
            "classifications_count",
            "retired_at",
            "retirement_criteria",
            "zoo_upload_date",
        ]
    ]

    subjects['clip_id'] = pd.read_sql_query("SELECT id FROM clips", conn)

    # test the validity of entries
    test_table(subjects, "subjects", keys=['id', 'clip_id'])

    # update the subjects table
    try:
        insert_many(conn, [tuple(i) for i in subjects.values], "subjects", 8)
    except sqlite3.Error as e:
        print(e)

    conn.commit()


if __name__ == "__main__":
    main()

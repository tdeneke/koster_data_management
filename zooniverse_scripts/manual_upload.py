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

    # Currently we discard sites that have no lat or lon coordinates, since site descriptions are not unique
    # it becomes difficult to match this information otherwise

    try:
        filename, ext = os.path.splitext(row["movie_filename"])
        filename = filename.rsplit("_", 1)[0]
        gid = retrieve_query(
            conn, f"SELECT id FROM movies WHERE filename=='{filename}'"
        )[0][0]
    except:
        gid = 0
    return gid


def test_table(table, db_table_name):
    try:
        # check that there are no id columns with a 0 value, which means that they were not matched
        assert len(table[(table == 0).any(axis=1)]) == 0
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
    last_date = "2020-01-10 00:00:00 UTC"
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

    # filter clip subjects
    man_data = man_data[man_data["metadata"].str.contains(".mp4")].reset_index()

    # flatten the metadata information
    flat_metadata = pd.json_normalize(man_data.metadata.apply(json.loads))

    # Select the filename of the clips
    clip_filenames = flat_metadata["filename"]

    # Get the starting time of clips in relation to the original movie
    # split the filename, select the last section, and remove the extension type
    flat_metadata["start_time"] = (
        clip_filenames.str.rsplit("_", 1).str[-1].str.replace(".mp4", "")
    )

    # Extract the filename of the original movie
    flat_metadata["movie_filename"] = flat_metadata.apply(
        lambda x: x["filename"].replace("_" + x["start_time"], "").replace(".mp4", ""), axis=1
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

    # Retrieve the id and filename from the movies table
    flat_metadata["movie_id"] = flat_metadata.apply(lambda x: get_id(conn, x), 1)

    # add movie_id to the flat metadata
    # clip_metadata = pd.merge(flat_metadata, movies, how = 'left',
    #                         left_on='movie_filename', right_on='filename')

    # Drop metadata column and define clip creation date as time uploaded to Zooniverse
    man_data = man_data.drop(columns="metadata")

    # Combine the information
    comb_data = pd.concat([man_data, flat_metadata], axis=1)

    # Select information to include in the clips table
    clips = comb_data.drop(
        columns=[
            "subject_id",
            "movie_filename",
            "workflow_id",
            "subject_set_id",
            "classifications_count",
            "retired_at",
            "retirement_reason",
        ]
    ).rename(columns={"created_at": "clipped_date", "index": "id"})

    # Combine the info to include in the subjects table
    subjects = comb_data.rename(
        columns={
            "created_at": "zoo_upload_date",
            "retirement_reason": "retirement_criteria",
            "index": "clip_id",
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
            "clip_id",
        ]
    ]

    # test the validity of entries

    test_table(clips, "clips")
    test_table(subjects, "subjects")

    # update the tables

    try:
        insert_many(conn, [tuple(i) for i in clips.values], "clips", 6)
        insert_many(conn, [tuple(i) for i in subjects.values], "subjects", 8)
    except sqlite3.Error as e:
        print(e)

    conn.commit()


if __name__ == "__main__":
    main()

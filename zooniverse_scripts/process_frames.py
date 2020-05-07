import os, io, csv, json
import requests, db_utils, argparse
import pandas as pd
from ast import literal_eval
from datetime import datetime
from panoptes_client import Project, Panoptes
from zooniverse_setup import auth_session
from collections import OrderedDict

# Specify the workflow of interest and its version
workflow_2 = 12852
workflow_2_version = 001.01


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

    # Get the export classifications
    export = project.get_export("classifications")

    # Save the response as pandas data frame
    rawdata = pd.read_csv(
        io.StringIO(export.content.decode("utf-8")),
        usecols=[
            "subject_ids",
            "subject_data",
            "classification_id",
            "workflow_id",
            "workflow_version",
            "annotations",
        ],
    )
    # Filter w2 classifications
    w2_data = rawdata[
        (rawdata.workflow_id >= workflow_2)
        & (rawdata.workflow_version >= workflow_2_version)
    ].reset_index()

    # Drop workflow columns
    w2_data = w2_data.drop(columns=["workflow_id", "workflow_version"])

    # Extract the video filename and annotation details
    w2_data["annotation"] = w2_data.apply(
        lambda x: (
            [v["filename"] for k, v in json.loads(x.subject_data).items()],
            literal_eval(x["annotations"])[0]["value"],
        )
        if len(literal_eval(x["annotations"])[0]["value"]) > 0
        else None,
        1,
    )

    # Convert annotation to format which the tracker expects
    ds = [
        OrderedDict(
            {
                "filename": i[0][0].split("_frame", 1)[0],
                "class_name": i[1][0]["tool_label"],
                "start_frame": int(i[0][0].split("_frame", 1)[1].replace(".jpg", "")),
                "x": int(i[1][0]["x"]),
                "y": int(i[1][0]["y"]),
                "w": int(i[1][0]["width"]),
                "h": int(i[1][0]["height"]),
            }
        )
        for i in w2_data.annotation
        if i is not None
    ]

    # Get prepared annotations
    w2_annotations = pd.DataFrame(ds)

    # Get species id for each species
    conn = db_utils.create_connection(args.db_path)

    species_df = pd.read_sql_query("SELECT id, label FROM species", conn)
    species_df = species_df.rename(columns={"id": "species_id"})

    w2_annotations = pd.merge(
        w2_annotations, species_df, how="left", left_on="class_name", right_on='label', validate="many_to_one"
    )

    # Validate movie ids to be added to db
    try:
        validation_df = pd.read_sql_table(
            f"SELECT filename, id FROM movies WHERE filename IN {w2_annotations['filename'].values}",
            conn,
        )
    except:
        validation_df = pd.DataFrame(columns=["filename", "id"])

    w2_annotations["movie_id"] = (
        w2_annotations["filename"]
        .reset_index()
        .merge(validation_df, on="filename", how="left")["id"]
    )

    w2_annotations = w2_annotations[w2_annotations.movie_id.notnull()][
        ["species_id", "x", "y", "w", "h", "start_frame", "species_id", "movie_id",]
    ]

    # Add values to agg_annotations_frame
    db_utils.add_to_table(
        args.db_path,
        "agg_annotations_frame",
        [(None,) + tuple(i) for i in w2_annotations.values],
        9,
    )

    print(f"Frame Aggregation Complete: {len(w2_annotations)} frames added")


if __name__ == "__main__":
    main()

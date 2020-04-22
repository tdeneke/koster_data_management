import argparse
from zooniverse_setup import *
from db_setup import *
import pandas as pd


def get_id(conn, row):

    # Currently we discard sites that have no lat or lon coordinates, since site descriptions are not unique
    # it becomes difficult to match this information otherwise

    try:
        gid = retrieve_query(conn, f"SELECT label FROM species WHERE id=='{row}'")[0][0]
    except:
        gid = None
    return gid


def clips_summary(conn):

    clips = pd.read_sql_query(f"SELECT * FROM agg_annotations_clip", conn)
    clips["species_name"] = clips["species_id"].apply(lambda x: get_id(conn, x), 1)

    return clips.groupby("species_name").agg({"species_id": "count", "how_many": "sum"})


def get_species_frames(species_name, conn):

    # Get species_id from name
    species_id = pd.read_sql_query(
        f"SELECT id FROM species WHERE label='{species_name}'", conn
    ).values[0][0]

    # Get clips for species from db
    clips = [
        i[0]
        for i in pd.read_sql_query(
            f"SELECT clip_id FROM agg_annotations_clip WHERE species_id={species_id}",
            conn,
        ).values
    ]

    # Get filenames of clips
    filenames = [
        i[0]
        for i in pd.read_sql_query(
            f"SELECT filename FROM clips WHERE id IN {tuple(clips)}", conn
        ).values
    ]

    return filenames


def wf2_upload(filenames, workflow_id, workflow_version):

    #Establish connection to Zooniverse

    #Create and upload subject sets to Zooniverse

    #Link subject sets to specific workflow
    return None


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
    conn = create_connection(args.db_path)

    print(get_species_frames("Fish (any species)", conn))

    # Upload clips to Zooniverse workflow
    # need help here


if __name__ == "__main__":
    main()


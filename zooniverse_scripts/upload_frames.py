import argparse
from zooniverse_setup import *
from db_setup import *
import pandas as pd
from clip_utils import *


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
        "--species", "-l", help="Species to upload", type=str, required=True
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

    # Connect to koster_db
    conn = create_connection(args.db_path)

    # Connect to Zooniverse
    koster_project = auth_session(args.user, args.password)

    # Get all movie_files and frame_numbers for species
    annotation_df = get_species_frames(args.species, conn)

    # Upload frames to Zooniverse (with movie metadata)
    # TO BE FILLED


if __name__ == "__main__":
    main()

# -*- coding: utf-8 -*-
import argparse, os, cv2, re
import utils.db_utils as db_utils
import pandas as pd
import numpy as np
import pims

from PIL import Image
from datetime import date
from utils.zooniverse_utils import auth_session
from panoptes_client import (
    SubjectSet,
    Subject,
    Project,
    Panoptes,
)
import utils.koster_utils as koster_utils

# Function to identify up to n number of frames per classified clip
# that contains species of interest after the first time seen
def get_species_frames(species_id, conn, n_frames):

    # Find classified clips that contain the species of interest
    frames_df = pd.read_sql_query(
        f"SELECT subject_id, first_seen FROM agg_annotations_clip WHERE agg_annotations_clip.species_id={species_id}",
        conn,
    )

    # Add species id to the df
    frames_df["frame_exp_sp_id"] = species_id

    # Get start time of the clips and ids of the original movies
    (frames_df["clip_start_time"], frames_df["movie_id"],) = list(
        zip(
            *pd.read_sql_query(
                f"SELECT clip_start_time, movie_id FROM subjects WHERE id IN {tuple(frames_df['subject_id'].values)} AND subject_type='clip'",
                conn,
            ).values
        )
    )

    # Identify the second of the original movie when the species first appears
    frames_df["first_seen_movie"] = (
        frames_df["clip_start_time"] + frames_df["first_seen"]
    )

    # Get the filepath and fps of the original movies
    f_paths = pd.read_sql_query(f"SELECT id, fpath, fps FROM movies", conn)

    # TODO: Fix fps figures for old movies and paths. Right now the path configuration is done manually to fix old copies
    f_paths["fps"] = f_paths["fps"].apply(lambda x: 25.0 if np.isnan(x) else x, 1)
    extensions = f_paths["fpath"].apply(lambda x: '' if len(os.path.splitext(x))>1 and os.path.splitext(x)[1]!='' else '.mov', 1)
    f_paths["fpath"] = f_paths["fpath"].apply(lambda x: os.path.basename(x), 1)
    f_paths["fpath"] = "/cephyr/NOBACKUP/groups/snic2021-6-9/movies/" + f_paths["fpath"] + extensions

    # Ensure swedish characters don't cause issues
    f_paths["fpath"] = f_paths["fpath"].apply(
        lambda x: str(x) if os.path.isfile(str(x)) else koster_utlis.unswedify(str(x))
    )
    # Include movies' filepath and fps to the df
    frames_df = frames_df.merge(f_paths, left_on="movie_id", right_on="id")

    # Specify if original movies can be found
    # frames_df["fpath"] = frames_df["fpath"].apply(lambda x: x.encode('utf-8'))
    frames_df["exists"] = frames_df["fpath"].map(os.path.isfile)

    if len(frames_df[~frames_df.exists]) > 0:
        print(
            f"There are {len(frames_df) - frames_df.exists.sum()} out of {len(frames_df)} frames with a missing movie"
        )

    # Select only frames from movies that can be found
    frames_df = frames_df[frames_df.exists]

    # Identify the ordinal number of the frames expected to be extracted
    frames_df["frame_number"] = frames_df[["first_seen_movie", "fps"]].apply(
        lambda x: [
            int((x["first_seen_movie"] + j) * x["fps"]) for j in range(n_frames)
        ],
        1,
    )

    # Reshape df to have each frame as rows
    lst_col = "frame_number"

    frames_df = pd.DataFrame(
        {
            col: np.repeat(frames_df[col].values, frames_df[lst_col].str.len())
            for col in frames_df.columns.difference([lst_col])
        }
    ).assign(**{lst_col: np.concatenate(frames_df[lst_col].values)})[
        frames_df.columns.tolist()
    ]

    # Drop unnecessary columns
    frames_df.drop(["subject_id"], inplace=True, axis=1)

    return frames_df


# Function to extract frames
def extract_frames(df, frames_folder):

    # Get movies filenames from their path
    df["movie_filename"] = df["fpath"].str.split("/").str[-1].str.replace(".mov", "")

    # Set the filename of the frames
    df["frame_path"] = (
        frames_folder
        + df["movie_filename"].astype(str)
        + "_frame_"
        + df["frame_number"].astype(str)
        + "_"
        + df["frame_exp_sp_id"].astype(str)
        + ".jpg"
    )

    # Read all original movies
    video_dict = {k: pims.Video(k) for k in df["fpath"].unique()}

    # Save the frame as matrix
    df["frames"] = df[["fpath", "frame_number"]].apply(
        lambda x: video_dict[x["fpath"]][int(x["frame_number"])],
        1,
    )

    # Extract and save frames
    for frame, filename in zip(df["frames"], df["frame_path"]):
        Image.fromarray(frame).save(f"{filename}")

    print("Frames extracted successfully")
    return df["frame_path"]


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
    parser.add_argument(
        "-fp",
        "--frames_folder",
        type=str,
        help="the absolute path to the folder to store frames",
        default=r"./frames",
        required=True,
    )
    parser.add_argument(
        "-t",
        "--testing",
        help="add flag if testing",
        required=False,
        action="store_true",
    )
    parser.add_argument(
        "-nf",
        "--n_frames",
        type=int,
        help="number of frames to create per clip",
        default=2,
        required=False,
    )

    args = parser.parse_args()

    # Connect to koster_db
    conn = db_utils.create_connection(args.db_path)

    # Connect to Zooniverse
    koster_project = auth_session(args.user, args.password)

    # Get id of species of interest
    species_id = pd.read_sql_query(
        f'SELECT id FROM species WHERE label="{args.species}"', conn
    ).values[0][0]

    # Identify n number of frames per classified clip that contains species of interest
    sp_frames_df = get_species_frames(species_id, conn, args.n_frames)

    # Get info of frames already uploaded
    uploaded_frames_df = pd.read_sql_query(
        f"SELECT movie_id, frame_number, frame_exp_sp_id FROM subjects WHERE frame_exp_sp_id='{species_id}' and subject_type='frame'",
        conn,
    )

    # Filter out frames that have already been uploaded
    if len(uploaded_frames_df) > 0 and not args.testing:

        # Exclude frames that have already been uploaded
        sp_frames_df = sp_frames_df[
            ~(sp_frames_df["movie_id"].isin(uploaded_frames_df["movie_id"]))
            & ~(sp_frames_df["frame_number"].isin(uploaded_frames_df["frame_number"]))
            & ~(
                sp_frames_df["frame_exp_sp_id"].isin(
                    uploaded_frames_df["frame_exp_sp_id"]
                )
            )
        ]

    # Upload frames to Zooniverse that have not been uploaded
    if len(sp_frames_df) == 0:
        print(
            "There are no subjects to upload, this may be because all of the subjects have already been uploaded"
        )
        raise

    else:
        # Create the folder to store the frames if not exist
        if not os.path.exists(args.frames_folder):
            os.mkdir(args.frames_folder)

        # Extract the frames and save them
        sp_frames_df["frame_path"] = extract_frames(sp_frames_df, args.frames_folder)
        sp_frames_df = sp_frames_df.drop_duplicates(subset=['frame_path'])

        # Select koster db metadata associated with each frame
        sp_frames_df["label"] = args.species
        sp_frames_df["subject_type"] = "frame"

        sp_frames_df = sp_frames_df[
            [
                "frame_path",
                "frame_number",
                "fps",
                "movie_id",
                "label",
                "frame_exp_sp_id",
                "subject_type",
            ]
        ]

        # Save the df as the subject metadata
        subject_metadata = sp_frames_df.set_index("frame_path").to_dict("index")

        # Create a subjet set in Zooniverse to host the frames
        subject_set = SubjectSet()

        subject_set.links.project = koster_project
        subject_set.display_name = args.species + date.today().strftime("_%d_%m_%Y")

        subject_set.save()

        print("Zooniverse subject set created")

        # Upload frames to Zooniverse (with metadata)
        new_subjects = []

        for filename, metadata in subject_metadata.items():
            subject = Subject()

            subject.links.project = koster_project
            subject.add_location(filename)

            subject.metadata.update(metadata)

            subject.save()
            new_subjects.append(subject)

        # Upload frames
        subject_set.add(new_subjects)

        print("Subjects uploaded to Zooniverse")


if __name__ == "__main__":
    main()

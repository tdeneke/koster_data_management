import argparse, os, cv2, re
import db_utils, clip_utils
import pandas as pd
import numpy as np
import pims

from PIL import Image
from datetime import date
from zooniverse_setup import auth_session
from panoptes_client import (
    SubjectSet,
    Subject,
    Project,
    Panoptes,
)  # needed to upload clips to Zooniverse


def get_fps(video_file):
    if isinstance(video_file, str):
        if os.path.isfile(video_file):
            fps = int(cv2.VideoCapture(video_file).get(cv2.CAP_PROP_FPS))
        else:
            video_file = unswedify(video_file)
            fps = int(cv2.VideoCapture(video_file).get(cv2.CAP_PROP_FPS))
    else:
        fps = None
    return fps


def get_species_frames(species_name, conn, movies_path):

    # Get species_id from name

    species_id = pd.read_sql_query(
        f"SELECT id FROM species WHERE label='{species_name}'", conn
    ).values[0][0]

    # Get clips for species from db
    frames_df = pd.read_sql_query(
        f"SELECT subject_id, first_seen FROM agg_annotations_clip WHERE agg_annotations_clip.species_id={species_id}",
        conn,
    )

    frames_df["frame_exp_sp_id"] = species_id

    # Get ids of movies
    (
        frames_df["start_time"],
        frames_df["end_time"],
        frames_df["movie_id"],
        frames_df["filename"],
    ) = list(
        zip(
            *pd.read_sql_query(
                f"SELECT clip_start_time, clip_end_time, movie_id, filename FROM subjects WHERE id IN {tuple(frames_df['subject_id'].values)} AND subject_type='clip'",
                conn,
            ).values
        )
    )

    # Get the filepath of the original movie
    f_paths = pd.read_sql_query(f"SELECT id, fpath FROM movies", conn)

    frames_df = frames_df.merge(f_paths, left_on="movie_id", right_on="id")

    frames_df["movie_filepath"] = frames_df["fpath"]

    # Identify the seconds in the original movie when the species appears
    frames_df["fps"] = frames_df["movie_filepath"].apply(get_fps, 1)

    frames_df["first_seen_movie"] = frames_df["start_time"] + frames_df["first_seen"]

    # Set the filename of the frames to extract
    frames_df["frame_number"] = frames_df["first_seen_movie"] * frames_df["fps"]
    frames_df["frame_number"] = frames_df["frame_number"].astype(int)

    frames_df.drop(["subject_id"], inplace=True, axis=1)

    return frames_df


# Function to extract up to three frames from movies after the first time seen
def extract_frames(df, frames_path, n_frames=3):

    # read all videos
    df["movie_filepath"] = df["movie_filepath"].apply(
        lambda x: str(x) if os.path.isfile(str(x)) else unswedify(str(x))
    )

    videos = [pims.Video(i) for i in df["movie_filepath"].unique().tolist()]
    video_dict = {k: v for k, v in zip(df["movie_filepath"].unique().tolist(), videos)}

    df["movie_filename"] = df["movie_filepath"].apply(
        lambda x: os.path.splitext(x)[0] if isinstance(x, str) else x, 1
    )

    df["frames"] = df[["movie_filepath", "first_seen_movie", "fps"]].apply(
        lambda x: video_dict[x["movie_filepath"]][
            np.arange(
                int(x["first_seen_movie"]) * int(x["fps"]),
                min(
                    int(x["first_seen_movie"]) * int(x["fps"])
                    + n_frames * int(x["fps"]),
                    len(video_dict[x["movie_filepath"]]),
                ),
                int(x["fps"]),
            )
        ],
        1,
    )
    df["frame_names"] = df[
        ["movie_base", "first_seen_movie", "fps", "frame_exp_sp_id"]
    ].apply(
        lambda x: [
            frames_path
            + "/"
            + x["movie_base"].replace(".mov", "")
            + "_frame_"
            + str(int(((x["first_seen_movie"] + j) * x["fps"])))
            + "_"
            + str(int(x["frame_exp_sp_id"]))
            + ".jpg"
            for j in range(n_frames)
        ],
        1,
    )

    # save frames to frame_names

    for frame, frame_name in zip(df["frames"].explode(), df["frame_names"].explode()):
        Image.fromarray(frame).save(f"{frame_name}")

    print("Frames extracted successfully")
    return df["frame_names"]


def unswedify(string):
    # Convert ä and ö to utf-8
    return (
        string.encode("utf-8")
        .replace(b"\xc3\xa4", b"a\xcc\x88")
        .replace(b"\xc3\xb6", b"a\xcc\x88")
        .decode("utf-8")
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
        "--frames_path",
        type=str,
        help="the absolute path to the folder to store frames",
        default=r"./frames",
        required=True,
    )
    parser.add_argument(
        "-mp",
        "--movies_path",
        type=str,
        help="the absolute path to the movie files",
        default=r"training_set_5_Jan2020",
        required=True,
    )
    args = parser.parse_args()

    # Connect to koster_db
    conn = db_utils.create_connection(args.db_path)

    # Connect to Zooniverse
    koster_project = auth_session(args.user, args.password)

    # Get all movie_files and frame_numbers for species
    annotation_df = get_species_frames(args.species, conn, args.movies_path)

    # Get species name
    species_id = pd.read_sql_query(
        f"SELECT id FROM species WHERE label='{args.species}'", conn
    ).values[0][0]

    # Get info of frames already classified
    uploaded_frames_df = pd.read_sql_query(
        f"SELECT movie_id, frame_number, frame_exp_sp_id FROM subjects WHERE frame_exp_sp_id='{species_id}' and subject_type='frame'",
        conn,
    )

    if len(uploaded_frames_df) > 0:

        # Exclude frames that have already been uploaded
        annotation_df = annotation_df[
            ~(annotation_df["movie_id"].isin(uploaded_frames_df["movie_id"]))
            & ~(annotation_df["frame_number"].isin(uploaded_frames_df["frame_number"]))
            & ~(
                annotation_df["frame_exp_sp_id"].isin(
                    uploaded_frames_df["frame_exp_sp_id"]
                )
            )
        ]

    # Create the folder to store the frames if not exist
    if not os.path.exists(args.frames_path):
        os.mkdir(args.frames_path)

    # Get valid movies base
    annotation_df["movie_base"] = annotation_df["movie_filepath"].apply(
        lambda x: os.path.basename(str(x))
        if os.path.isfile(x)
        else unswedify(os.path.basename(str(x))),
        1,
    )

    annotation_df = annotation_df[
        annotation_df["movie_base"].isin(os.listdir(args.movies_path))
    ]

    # Extract the frames and save them
    f_paths = extract_frames(annotation_df, args.frames_path, 2)

    # Rename the filename column
    annotation_df = annotation_df.rename(columns={"frame_names": "filename"})

    # Create a subjest in Zooniverse where the frames will be uploaded
    subject_set = SubjectSet()

    subject_set.links.project = koster_project
    subject_set.display_name = args.species + date.today().strftime("_%d_%m_%Y")

    subject_set.save()

    # Save information relevant to the subjects table of the koster db
    annotation_df["label"] = args.species

    annotation_df["metadata"] = annotation_df[
        [
            "movie_filepath",
            "frame_number",
            "fps",
            "movie_id",
            "label",
            "frame_exp_sp_id",
        ]
    ].to_dict("r")

    annotation_df["frame_paths"] = f_paths

    annotation_df = annotation_df.drop(
        [i for i in annotation_df.columns if i not in ["metadata", "frame_paths"]], 1
    )
    annotation_df = annotation_df[["frame_paths", "metadata"]].dropna()

    # Upload frames to Zooniverse (with metadata)
    new_subjects = []

    for filename, metadata in annotation_df.values:

        for f in range(len(filename)):

            metadata["filename"] = filename[f]
            metadata["frame_number"] = metadata["frame_number"] + f * metadata["fps"]

            subject = Subject()

            subject.links.project = koster_project  # tutorial_project
            subject.add_location(filename[f])

            subject.metadata.update(metadata)

            subject.save()
            new_subjects.append(subject)

    # Upload frames
    subject_set.add(new_subjects)


if __name__ == "__main__":
    main()

import argparse, os, cv2, re
import db_utils, clip_utils
import pandas as pd
import numpy as np
from mydia import Videos
from PIL import Image
from datetime import date
from zooniverse_setup import auth_session


def get_fps(video_file):
    try:
        fps = int(cv2.VideoCapture(video_file).get(cv2.CAP_PROP_FPS))
    except:
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
        conn
    )

    frames_df["frame_exp_sp_id"] = species_id

    # Get ids of movies
    frames_df["start_time"], frames_df["end_time"], frames_df["movie_id"], frames_df["filename"] = list(
        zip(
            *pd.read_sql_query(
                f"SELECT clip_start_time, clip_end_time, movie_id, filename FROM subjects WHERE id IN {tuple(frames_df['subject_id'].values)} AND subject_type='clip'",
                conn,
            ).values
        )
    )

    # Get the filepath of the original movie
    frames_df["movie_filepath"] = pd.read_sql_query(
        f"SELECT fpath FROM movies WHERE id IN {tuple(frames_df['movie_id'].values)}",
        conn,
    )

    # Temp solution: get correct version of filename from folder
    frames_df['movie_base'] = frames_df['movie_filepath'].apply(lambda x: unswedify(os.path.basename(str(x))), 1)
    frames_df = frames_df[frames_df["movie_base"].isin(os.listdir(movies_path))]

    # Identify the seconds in the original movie when the species appears
    frames_df["fps"] = frames_df["m_fpath"].apply(get_fps, 1)
    frames_df["first_seen_movie"] = frames_df["start_time"] // frames_df["fps"]  + frames_df["first_seen"]

    # Set the filename of the frames to extract
    frames_df["movie_frame"] = frames_df["filename"].apply(
        lambda x: int(re.findall(r"(?<=_)\d+", x)[0])
    )

    frames_df.drop(["subject_id"], inplace=True, axis=1)

    return frames_df

# Function to extract up to three frames from movies after the first time seen
def extract_frames(df, frames_path, n_frames=3):
    # read all videos
    reader = Videos()
    videos = reader.read(df["m_fpath"].tolist(), workers=8)
    video_dict = {k:v for k,v in zip(df.groupby(["movie_filename"]).groups.keys(), videos)}
    df["movie_filename"] = df["movie_filepath"].apply(
        lambda x: os.path.splitext(x)[0] if isinstance(x, str) else x, 1
    )
    df["frames"] = df[["movie_filename", "first_seen_movie", "fps"]].apply(lambda x: video_dict[x['movie_filename']][np.arange(x['first_seen_movie'], x['first_seen_movie'] + 3*x['fps'], x['fps'])], 1)
    df["frame_names"] = pd.Series([frames_path
                    + "/"
                    + df["movie_filename"]
                    + "_frame_"
                    + ((df['first_seen_movie'] + j) * df['fps']).astype(str) + ".jpg" for j in range(n_frames)])
    
    # save frames to frame_names

    for frame, frame_name in zip(df['frames'].explode(), df['frame_names'].explode()):
        Image.fromarray(frame).save(
                f"{frame_name}"
        )

    print("Frames extracted successfully")
    return df["frame_names"]


def unswedify(string):
    return string.encode('utf-8').replace(b'a\xcc\x88', b'\xc3\xa4')


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
            ~(annotation_df["movie_id"] == uploaded_frames_df["movie_id"])
            & ~(annotation_df["frame_number"] == uploaded_frames_df["frame_number"])
            & ~(
                annotation_df["frame_exp_sp_id"]
                == uploaded_frames_df["frame_exp_sp_id"]
            )
        ]

    # Create the folder to store the frames if not exist
    if not os.path.exists(args.frames_path):
        os.mkdir(args.frames_path)

    # Get valid movies
    annotation_df['movie_base'] = annotation_df['movie_filepath'].apply(lambda x: unswedify(os.path.basename(str(x))), 1)
    annotation_df = annotation_df[annotation_df["movie_base"].isin(os.listdir(args.movies_path))]
    print(annotation_df.head())

    # Extract the frames and save them
    f_paths = extract_frames(annotation_df, args.frames_path, 3)

    # Create a subjest in Zooniverse where the frames will be uploaded
    subject_set = SubjectSet()

    subject_set.links.project = koster_project
    subject_set.display_name = args.species + date.today().strftime("_%d_%m_%Y")

    subject_set.save()

    # Save the columns with information about the frames as metadata
    annotation_df["metadata"] = annotation_df[
        ["frame_number", "movie_id", "frame_exp_sp_id"]
    ].to_dict("r")

    annotation_df['frame_paths'] = f_paths

    annotation_df = annotation_df.drop([i for i in annotation_df.columns if i not in ['metadata', 'frame_paths']], 1)

    # Upload frames to Zooniverse (with metadata)
    new_subjects = []

    for filename, metadata in annotation_df.values:
        subject = Subject()

        subject.links.project = tutorial_project
        subject.add_location(filename)

        subject.metadata.update(metadata)

        subject.save()
        new_subjects.append(subject)

    # Upload frames
    subject_set.add(new_subjects)


if __name__ == "__main__":
    main()

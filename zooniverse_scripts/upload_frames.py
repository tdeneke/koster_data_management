import argparse, os, cv2
import db_utils, clip_utils
import pandas as pd
from mydia import Videos
from PIL import Image
from datetime import date
from zooniverse_setup import auth_session


def get_fps(video_file):
    return int(cv2.VideoCapture(video_file).get(cv2.CAP_PROP_FPS))

def get_species_frames(species_name, conn):

    # Get species_id from name

    species_id = pd.read_sql_query(
        f"SELECT id FROM species WHERE label='{species_name}'", conn
    ).values[0][0]

    # Get clips for species from db
    frames_df = pd.read_sql_query(
        f"SELECT clip_id, first_seen, start_time, end_time FROM agg_annotations_clip LEFT JOIN clips ON agg_annotations_clip.clip_id = clips.id WHERE agg_annotations_clip.species_id={species_id}",
        conn,
    )

    frames_df["expected_species"] = species_id

    # Get ids of movies
    frames_df["movie_id"], frames_df["filename"] = list(
        zip(
            *pd.read_sql_query(
                f"SELECT movie_id, filename FROM clips WHERE id IN {tuple(frames_df['clip_id'].values)}",
                conn,
            ).values
        )
    )

    # Identify the seconds in the original movie when the species appears
    frames_df["fps"] = frames_df["filename"].apply(get_fps, 1)
    frames_df["first_seen_movie"] = frames_df["start_time"] // frames_df["fps"]  + frames_df["first_seen"]

    # Get the filepath of the original movie
    frames_df["movie_filename"] = pd.read_sql_query(
        f"SELECT fpath FROM movies WHERE id IN {tuple(frames_df['movie_id'].values)}",
        conn,
    )

    # Set the filename of the frames to extract
    frames_df["movie_frame"] = frames_df["filename"].apply(
        lambda x: int(re.findall(r"(?<=_)\d+", x)[0])
    )

    frames_df.drop(["clip_id"], inplace=True, axis=1)

    return frames_df

# Function to extract up to three frames from movies after the first time seen
def extract_frames(df, frames_path, n_frames=3):
    # read all videos
    reader = Videos()
    videos = reader.read(df["filename"].tolist(), workers=8)
    m_names = df["movie_filename"].apply(
        lambda x: os.path.splitext(x)[0] if isinstance(x, str) else x, 1
    )
    f_paths = [frames_path
                    + "/"
                    + m_names[i]
                    + "_frame_"
                    + ((df['first_seen_movie'] + j) * df['fps']).astype(str) + ".jpeg" for j in range(n_frames)]

    for i in range(len(videos)):
        for j in range(n_frames):
            try:
                frame = videos[:, (df["first_seen_movie"].iloc[i] + j) * df["fps"].iloc[i], ...][
                    i
                ]
                # save image in filepath
                Image.fromarray(frame).save(f"{f_paths[i][j]}")
            except:
                pass

    print("Frames extracted successfully")
    return f_paths


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
    args = parser.parse_args()

    # Connect to koster_db
    conn = db_utils.create_connection(args.db_path)

    # Connect to Zooniverse
    koster_project = auth_session(args.user, args.password)

    # Get all movie_files and frame_numbers for species
    annotation_df = get_species_frames(args.species, conn)

    # Get species name
    species_id = pd.read_sql_query(
        f"SELECT id FROM species WHERE label='{args.species}'", conn
    ).values[0][0]

    # Get info of frames already classified
    uploaded_frames_df = pd.read_sql_query(
        f"SELECT movie_id, frame_number, expected_species FROM agg_annotations_frame WHERE species_id='{species_id}'",
        conn,
    )

    if len(uploaded_frames_df) > 0:

        # Exclude frames that have already been uploaded
        annotation_df = annotation_df[
            ~(annotation_df["movie_id"] == uploaded_frames_df["movie_id"])
            & ~(annotation_df["movie_frame"] == uploaded_frames_df["frame_number"])
            & ~(
                annotation_df["expected_species"]
                == uploaded_frames_df["expected_species"]
            )
        ]

    # Create the folder to store the frames if not exist
    if not os.path.exists(args.frames_path):
        os.mkdir(args.frames_path)

    # Extract the frames and save them
    f_paths = extract_frames(annotation_df, args.frames_path, 3)

    # Create a subjest in Zooniverse where the frames will be uploaded
    subject_set = SubjectSet()

    subject_set.links.project = koster_project
    subject_set.display_name = args.species + date.today().strftime("_%d_%m_%Y")

    subject_set.save()

    # Save the columns with information about the frames as metadata
    annotation_df["metadata"] = annotation_df[
        ["movie_frame", "movie_id", "expected_species"]
    ].to_dict("r")

    annotation_df['frame_paths'] = pd.Series(f_paths)

    annotation_df = annotation_df.drop(columns=[i for i in annotation_df.columns if i not in ['metadata', 'frame_paths']], 1)

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

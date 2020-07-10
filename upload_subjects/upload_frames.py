import argparse, os, cv2, re
import utils.db_utils as db_utils
import utils.clip_utils as clip_utils
import pandas as pd
import numpy as np

from datetime import date
from tqdm import tqdm
from utils.zooniverse_utils import auth_session
from panoptes_client import (
    SubjectSet,
    Subject,
    Project,
    Panoptes,
)

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
    frames_df["first_seen_movie"] = frames_df["clip_start_time"] + frames_df["first_seen"]

    # Get the filepath and fps of the original movies
    f_paths = pd.read_sql_query(f"SELECT id, fpath, fps FROM movies", conn)

    # Ensure swedish characters don't cause issues
    f_paths["fpath"] = f_paths["fpath"].apply(
        lambda x: str(x) if os.path.isfile(str(x)) else db_utils.unswedify(str(x))
    )
    
    # Include movies' filepath and fps to the df
    frames_df = frames_df.merge(f_paths, left_on="movie_id", right_on="id")
    
    # Specify if original movies can be found
    frames_df['exists'] = frames_df['fpath'].map(os.path.isfile)
    
    if len(frames_df[~frames_df.exists]) > 0:
        print(
            f"There are {len(frames_df) - frames_df.exists.sum()} out of {len(frames_df)} frames with a missing movie"
        )
        
    # Select only frames from movies that can be found
    frames_df = frames_df[frames_df.exists]
    
    # Identify the ordinal number of the frames expected to be extracted
    frames_df["frame_number"] = frames_df[
        ["first_seen_movie", "fps"]
    ].apply(
        lambda x: [
            int((x["first_seen_movie"] + j) * x["fps"])
            for j in range(n_frames)
        ],
        1,
    )
    
    # Reshape df to have each frame as rows
    lst_col = 'frame_number'

    frames_df = pd.DataFrame({
        col:np.repeat(frames_df[col].values, frames_df[lst_col].str.len())
        for col in frames_df.columns.difference([lst_col])
    }).assign(**{lst_col:np.concatenate(frames_df[lst_col].values)})[frames_df.columns.tolist()]
    
    # Drop unnecessary columns
    frames_df.drop(["subject_id"], inplace=True, axis=1)
    
    return frames_df
    
# Function to extract frames 
def extract_frames(df, frames_path):    

    # Get valid movies filenames
    df["movie_filename"] = df["fpath"].apply(
        lambda x: os.path.splitext(x)[0] if isinstance(x, str) else x, 1
    )
    
    # Set the filename of the frames
    df["filename"] = (frames_path
                      + df["movie_filename"].replace(".mov", "")
                      + "_frame_"
                      + df["frame_number"].astype(str)
                      + "_"
                      + df["frame_exp_sp_id"].astype(str)
                      + ".jpg"
                     )
    
    print(df.head)
    
    for index, row in df.iterrows():
        
        # Read the video
        vidcap = cv2.VideoCapture(row["movie_filename"])
        
        # Set the frame to extract
        vidcap.set(1,row["frame_number"])
        
        # Extract the frame
        ret, frame = vidcap.read()
        cv2.imwrite(row["filename"], frame)
        
        print('Frame ', row["frame_number"], " from ", row["movie_filename"]," was saved to ", row["filename"])
        
        vidcap.release()
    
    

    print("Frames extracted successfully")
    return df["filename"]


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
        f"SELECT id FROM species WHERE label='{args.species}'", conn
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
        if not os.path.exists(args.frames_path):
            os.mkdir(args.frames_path)
        
        # Extract the frames and save them
        f_paths = extract_frames(sp_frames_df, args.frames_path)

        # Create a subjet set in Zooniverse to host the frames
        subject_set = SubjectSet()

        subject_set.links.project = koster_project
        subject_set.display_name = args.species + date.today().strftime("_%d_%m_%Y")

        subject_set.save()

        print("Zooniverse subject set created")
            
        # Save information relevant to the subjects table of the koster db
        sp_frames_df["label"] = args.species
        sp_frames_df["subject_type"] = "frame"

        sp_frames_df["metadata"] = sp_frames_df[
            [
                "fpath",
                "frame_number",
                "fps",
                "movie_id",
                "label",
                "frame_exp_sp_id",
                "subject_type",
            ]
        ].to_dict("r")

        sp_frames_df["frame_paths"] = f_paths

        sp_frames_df = sp_frames_df.drop(
            [i for i in sp_frames_df.columns if i not in ["metadata", "frame_paths"]], 1
        ).dropna()

        # Upload frames to Zooniverse (with metadata)
        new_subjects = []

        for filename, metadata in tqdm(sp_frames_df.values):

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

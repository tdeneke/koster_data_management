import argparse, os, cv2, re
import utils.db_utils as db_utils
import pandas as pd
import numpy as np
import math
import subprocess

from datetime import date
from utils.zooniverse_utils import auth_session
from panoptes_client import (
    SubjectSet,
    Subject,
    Project,
    Panoptes,
)

def expand_list(df, list_column, new_column): 
    lens_of_lists = df[list_column].apply(len)
    origin_rows = range(df.shape[0])
    destination_rows = np.repeat(origin_rows, lens_of_lists)
    non_list_cols = (
      [idx for idx, col in enumerate(df.columns)
       if col != list_column]
    )
    expanded_df = df.iloc[destination_rows, non_list_cols].copy()
    expanded_df[new_column] = (
      [item for items in df[list_column] for item in items]
      )
    expanded_df.reset_index(inplace=True, drop=True)
    return expanded_df


def get_clips(n_clips, conn, video_interest):
    
    # Get information of the movies to upload new clips from
    if video_interest:
        # Select only the movies of interest if specified
        available_movies_df = pd.read_sql_query(
            f"SELECT id, fps, duration, fpath FROM movies WHERE id={video_interest}",
            conn,
        )    
    else:
        # Select all movies
        available_movies_df = pd.read_sql_query(
            f"SELECT id, fps, duration, fpath FROM movies",
            conn,
        )
        
    # Rename the id of the movies to avoid confusion
    available_movies_df = available_movies_df.rename(columns={"id": "movie_id"})
    
    # Set the start of each movie of interest
    available_movies_df["start"] = 0
    
    # Convert the "duration" column to integer
    available_movies_df["duration"] = available_movies_df["duration"].astype(int)
    
    # Calculate all the potential seconds for the new clips to start
    available_movies_df["seconds"] = [list(range(i, int(math.floor(j/clip_length)*clip_length), clip_length)) for i, j in available_movies_df[['start','duration']].values]
    
    # Reshape the dataframe of potential seconds for the new clips to start
    potential_start_df = expand_list(available_movies_df, "seconds", "pot_seconds")
    
    # Get information of clips uploaded 
    uploaded_clips_df = pd.read_sql_query(
        f"SELECT movie_id, clip_start_time, clip_end_time FROM subjects WHERE subject_type='clip' AND movie_id IN {tuple(available_movies_df['movie_id'].values)}",
        conn,
    )
    
    # Calculate the time when the new clips shouldn't start to avoid duplication (min=0)
    uploaded_clips_df["clip_start_time"] = (uploaded_clips_df["clip_start_time"]-clip_length).clip(lower=0)
                      
    # Calculate all the seconds when the new clips shouldn't start
    uploaded_clips_df["seconds"] = [list(range(i, j+1)) for i, j in uploaded_clips_df[['clip_start_time','clip_end_time']].values]
      
    # Reshape the dataframe of the seconds when the new clips shouldn't start
    uploaded_start = expand_list(uploaded_clips_df, "seconds", "upl_seconds")[["movie_id","upl_seconds"]]
        
    # Exclude starting times of clips that have already been uploaded
    potential_clips_df = pd.merge(potential_start_df, uploaded_start, 
                                  how='left', left_on= ["movie_id","pot_seconds"], 
                                  right_on= ["movie_id","upl_seconds"],
                                  indicator=True).query('_merge == "left_only"').drop(columns=['_merge'])
    
    # Sample up to n clips 
    new_clips_df = potential_clips_df.sample(n=n_clips)
    
    # Select only relevant clomuns
    clips_df = new_clips_df[["movie_id","fps","fpath","pot_seconds"]]
    
    return clips_df
    
# Function to extract the clips 
def extract_clips(df, clips_folder, clip_length):    

    # Get movies filenames from their path
    df["movie_filename"] = df["fpath"].str.split('/').str[-1].str.replace(".mov", "")
    
    # Set the filename of the clips
    df["clip_path"] = (clips_folder
                       + "/"
                       + df["movie_filename"].astype(str)
                       + "_clip_"
                       + df["pot_seconds"].astype(str)
                       + "_"
                       + str(clip_length)
                       + ".mp4"
                      )
    
    # Read each movie and extract the clips
    for movie in df.fpath:
        movie_df = df[df['fpath'] == movie]
        for i in range(len(movie_df.index)):
            subprocess.call(["ffmpeg", "-ss", str(movie_df.iloc[i]['pot_seconds']), "-t", str(clip_length), "-i", movie, "-c", "copy", "-force_key_frames", "1", str(movie_df.iloc[i]['clip_path'])])
            
    print("clips extracted successfully")
    return df["clip_path"]    
    
    
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
    parser.add_argument(
        "-fp",
        "--clips_folder",
        type=str,
        help="the absolute path to the folder to store clips",
        default=r"./clips",
        required=True,
    )
    parser.add_argument(
        "-n",
        "--n_clips",
        help="Number of clips to sample", 
        type=int, 
        required=True,
    )
    parser.add_argument(
        "-lg",
        "--clip_length",
        help="Length in seconds of each clip", 
        default=10,
        type=int, 
        required=False,
    )
    parser.add_argument(
        "-vi",
        "--video_interest",
        help="Id of a video of interest to get the clips from", 
        type=str, 
        required=False,
    )
    
    
    args = parser.parse_args()
    
    # Set the clip of the length if specified
    if args.clip_length:
        clip_length=args.clip_length

    # Connect to koster_db
    conn = db_utils.create_connection(args.db_path)
    
    # Connect to Zooniverse
    koster_project = auth_session(args.user, args.password)

    # Identify n number of clips that haven't been uploaded to Zooniverse 
    clips_df = get_clips(args.n_clips, conn, args.video_interest)

    # Create the folder to store the clips if not exist
    if not os.path.exists(args.clips_folder):
        os.mkdir(args.clips_folder)
     
    # Extract the clips and store them in the folder
    clips_df["clip_path"] = extract_clips(clips_df, args.clips_folder, clip_length)
    
 
    # Select koster db metadata associated with each clip
    clips_df["clip_start_time"] = clips_df["pot_seconds"]
    clips_df["clip_end_time"] = clips_df["pot_seconds"]+clip_length
    clips_df["subject_type"] = "clip"

    clips_df = clips_df[
        [
            "clip_path",
            "fpath",
            "clip_start_time",
            "clip_end_time",
            "fps",
            "movie_id",
            "subject_type",
        ]
    ]

    # Save the df as the subject metadata
    subject_metadata = clips_df.set_index('clip_path').to_dict('index')

    # Create a subjet set in Zooniverse to host the frames
    subject_set = SubjectSet()

    subject_set.links.project = koster_project
    subject_set.display_name = "clips" + date.today().strftime("_%d_%b_%Y")

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

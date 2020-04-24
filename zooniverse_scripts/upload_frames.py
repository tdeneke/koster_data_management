import argparse
from zooniverse_setup import *
from db_setup import *
import pandas as pd
from clip_utils import *


def get_species_frames(species_name, conn):

    # Get species_id from name
    species_id = pd.read_sql_query(
        f"SELECT id FROM species WHERE label='{species_name}'", conn
    ).values[0][0]

    # Get clips for species from db
    frames_df = pd.read_sql_query(
        f"SELECT clip_id, first_seen, start_time, end_time FROM agg_annotations_clip WHERE species_id={species_id}",
        conn,
    )

    frames_df["expected_species"] = species_id

    # Identify the seconds in the original movie when the species appears
    frames_df["first_seen_movie"] = frames_df["start_time"] + frames_df["first_seen"]
    
    # Get ids of movies
    frames_df["movie_id"], frames_df["filename"] = list(
        zip(
            *pd.read_sql_query(
                f"SELECT movie_id, filename FROM clips WHERE id IN {tuple(frames_df['clip_id'].values)}",
                conn,
            ).values
        )
    )

    # Get the filepath of the original movie
    frames_df["movie_filename"] = pd.read_sql_query(
        f"SELECT fpath FROM movies WHERE id IN {tuple(frames_df['movie_id'].values)}",
        conn,
    )
    
    # Set the filename of the frames to extract
    frames_df["movie_frame"] = np.round(
        frames_df["first_seen_movie"]
        + frames_df["filename"].apply(lambda x: int(re.findall(r"(?<=_)\d+", x)[0]))
    )

    frames_df.drop(
        ["movie_id", "clip_id", "filename", "first_seen"], inplace=True, axis=1
    )

    return frames_df


#Function to extract up to three frames from movies after the first time seen
def get_fps(df, frames_path):

    

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
        default=r"/frames",
        required=True,
    )
    args = parser.parse_args()

    # Connect to koster_db
    conn = create_connection(args.db_path)

    # Connect to Zooniverse
    koster_project = auth_session(args.user, args.password)

    # Get all movie_files and frame_numbers for species
    annotation_df = get_species_frames(args.species, conn)

    # Get info of frames already classified
    uploaded_frames_df = pd.read_sql_query(
        f"SELECT movie_id, frame_number, expected_species FROM agg_annotations_frame WHERE label='{species_name}'", conn
    ).values[0][0]
    
    # Exclude frames that have already been uploaded
    annotation_df = annotation_df[
        ~(annotation_df.movie_id = uploaded_frames_df.movie_id) &
        ~(annotation_df.frame_number = uploaded_frames_df.frame_number)&
        ~(annotation_df.expected_species = uploaded_frames_df.expected_species)
    ]
    
    # Create the folder to store the frames if not exist
    if not os.path.exists(args.frames_path):
        os.mkdir(args.frames_path)

    # Extract the frames and save them
    #get_fps(annotation_df,args.frames_path)
    
    # Save the manuscript with metadata information about the frames
    manuscript = annotation_df[
        ["movie_frame", "movie_id", "frame_number", "expected_species"]
    ]
    manuscript.to_csv(args.frames_path+"manuscript.csv",index=False)
    
    # Upload frames to Zooniverse (with movie metadata)
    # TO BE FILLED


if __name__ == "__main__":
    main()


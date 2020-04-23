import argparse, re
from zooniverse_setup import *
from db_setup import *
import pandas as pd
import numpy as np


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
    frames_df = pd.read_sql_query(
        f"SELECT clip_id, first_seen FROM agg_annotations_clip WHERE species_id={species_id}",
        conn,
    )

    frames_df["species_id"] = species_id

    # Get ids of movies
    frames_df["movie_id"], frames_df["filename"] = list(
        zip(
            *pd.read_sql_query(
                f"SELECT movie_id, filename FROM clips WHERE id IN {tuple(frames_df['clip_id'].values)}",
                conn,
            ).values
        )
    )

    frames_df["movie_filename"] = pd.read_sql_query(
        f"SELECT fpath FROM movies WHERE id IN {tuple(frames_df['movie_id'].values)}",
        conn,
    )
    frames_df["movie_frame"] = np.round(
        frames_df["first_seen"]
        + frames_df["filename"].apply(lambda x: int(re.findall(r"(?<=_)\d+", x)[0]))
    )

    frames_df.drop(
        ["movie_id", "clip_id", "filename", "first_seen"], inplace=True, axis=1
    )

    return frames_df

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

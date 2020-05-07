import argparse, re
import db_utils
import pandas as pd
import numpy as np


def clips_summary(conn):

    clips = pd.read_sql_query(f"SELECT * FROM agg_annotations_clip", conn)
    clips["species_name"] = clips["species_id"].apply(lambda x: get_id(conn, x), 1)

    return clips.groupby("species_name").agg({"species_id": "count", "how_many": "sum"})

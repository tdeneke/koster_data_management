import argparse, re
import pandas as pd
import numpy as np
import utils.db_utils as db_utils

def get_sp_label(conn, row):
    try:
        gid = retrieve_query(
            conn, f"SELECT label FROM species WHERE id=='{row}'"
        )[0][0]
    except:
        gid = None
    return gid

def clips_summary(db_path):

    conn = db_utils.create_connection(db_path)
    
    clips = pd.read_sql_query(f"SELECT * FROM agg_annotations_clip", conn)
    clips["species_name"] = clips["species_id"].apply(lambda x: get_sp_label(conn, x), 1)

    return clips.groupby("species_name").agg({"species_id": "count", "how_many": "sum"})

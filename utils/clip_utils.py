import argparse, re
import pandas as pd
import numpy as np
import utils.db_utils as db_utils

def clips_summary(db_path):

    conn = db_utils.create_connection(db_path)
    
    clips = pd.read_sql_query(f"SELECT * FROM agg_annotations_clip", conn)
    
    # Get id of species of interest
    species_id = pd.read_sql_query(f"SELECT id, label FROM species", conn)
    
    # Include a column with unique ids for duplicated subjects 
    clips = pd.merge(clips, species_id, how="left", left_on="species_id", right_on="id")

    return clips.groupby("label").agg({"species_id": "count", "how_many": "sum"})

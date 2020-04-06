import os, csv, json, sys
import operator, argparse
import requests
import pandas as pd
import sqlite3
from datetime import datetime
from db_setup import *
from zooniverse_setup import *


def add_movies(file)
	
	movies_df = pd.read_csv(file)

	# Set up sites information
	sites_df = movies_df[['SiteDecription', 'CentroidLat', 'CentroidLong']].groupby(['CentroidLat', 'CentroidLong']).min().reset_index()
	sites_db = sites_df[['SiteDecription', 'CentroidLat', 'CentroidLong']]

	# Update sites table
	conn = create_connection(args.db_path)

    try:
        insert_many(conn, [tuple(i) + (None, ) for i in site_db.values], "sites", 5)
    except sqlite3.Error as e:
        print(e)

    conn.commit()

    # Reference with sites table

	movies_df['Site_id'] = movies_df.apply(lambda x: execute_query(f"SELECT id FROM sites WHERE coord_lat=={x['CentroidLat']} AND coord_lon=={x['CentroidLong']} "))

	movies_db = movies_df[['FilenameCurrent', 'DateFull', 'Total_time', 'Author', 'Site_id']]

	# Update movies table
	conn = create_connection(args.db_path)

    try:
        insert_many(conn, [tuple(i) + (None, ) for i in movies_db.values], "movies", 6)
    except sqlite3.Error as e:
        print(e)

    conn.commit()

    print('Updated sites and movies')



def add_species(file):

	species_df = pd.read_csv(file)

	# Update movies table
	conn = create_connection(args.db_path)

    try:
        insert_many(conn, [tuple(i) for i in species_df['Name'].values], "species", 1)
    except sqlite3.Error as e:
        print(e)

    conn.commit()

    print('Updated species')


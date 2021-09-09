import argparse, os
import utils.db_utils as db_utils
import pandas as pd
import numpy as np
import math
from IPython.display import HTML, display, update_display, clear_output
import ipywidgets as widgets
from ipywidgets import interact

def choose_movies(db_path, subjects):
    
    # Connect to db
    conn = db_utils.create_connection(db_path)
    
    # Select all movies
    movies_df = pd.read_sql_query(
        f"SELECT filename, fpath FROM movies",
        conn,
    )
    
    # Select only videos that can be mapped
    available_movies_df = movies_df[movies_df['fpath'].map(os.path.isfile)]
    
    ###### Select movies ####
    # Display the movies available to upload
    movie_selection = widgets.Combobox(
        options = available_movies_df.filename.unique().tolist(),
        description = 'Movie:',
    )
    
    # Display the selected movie 
    #caption_movie = widgets.Label(value='Select a movie to upload')
    
    #def handle_movie_change(change):
    #    caption_movie.value = 'The selected movie is ' + change.new
    
    #movie_to_upload.observe(handle_movie_change, names='value')
    
    ###### Select clip length ##########
    # Display the length available
    clip_length = widgets.RadioButtons(
        options = [5,10],
        value = 10,
        description = 'Clip length (seconds):',
    )
    
    display(movie_selection, clip_length)
    
    return movie_selection, clip_length

def choose_clips(db_path, movie_selection, clip_length):
    
    # Connect to db
    conn = db_utils.create_connection(db_path)
    
    # Select the movie to upload
    movie_df = pd.read_sql_query(
        f"SELECT filename, fps, survey_start, survey_end FROM movies WHERE movies.filename='{movie_selection}'",
        conn,
    )
    
    # Estimate the maximum number of clips available
    survey_end_movie = movie_df["survey_end"].values[0]
    max_n_clips = math.floor(survey_end_movie/clip_length)
    
    ###### Select number of clips ##########
    # Display the number of potential clips available
    n_clips = widgets.IntSlider(
        value=max_n_clips,
        min=0,
        max=max_n_clips,
        step=1,
        description = 'Number of clips to upload:',
    )
    
    display(n_clips)
    
    return n_clips
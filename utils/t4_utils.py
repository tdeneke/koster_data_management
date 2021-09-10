#t4 utils
import argparse, os
import utils.db_utils as db_utils
import pandas as pd
import numpy as np
import math
from IPython.display import HTML, display, update_display, clear_output
import ipywidgets as widgets
from ipywidgets import interact
from utils.zooniverse_utils import auth_session

def choose_movies(db_path):
    
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
    
    ###### Select clip length ##########
    # Display the length available
    clip_length = widgets.RadioButtons(
        options = [5,10],
        value = 10,
        description = 'Clip length (seconds):',
    )
    
    display(movie_selection, clip_length)
    
    return movie_selection, clip_length

def choose_clips(movie_selection, clip_length, db_path):
    
    # Connect to db
    conn = db_utils.create_connection(db_path)
    
    # Select the movie to upload
    movie_df = pd.read_sql_query(
        f"SELECT id, filename, fps, survey_start, survey_end FROM movies WHERE movies.filename='{movie_selection}'",
        conn,
    )
    
    print(movie_df.id.values)
    
    # Get information of clips uploaded
    uploaded_clips_df = pd.read_sql_query(
        f"SELECT movie_id, clip_start_time, clip_end_time FROM subjects WHERE subjects.subject_type='clip' AND subjects.movie_id={movie_df.id.values}",
        conn,
    )

    # Calculate the time when the new clips shouldn't start to avoid duplication (min=0)
    uploaded_clips_df["clip_start_time"] = (
        uploaded_clips_df["clip_start_time"] - clip_length
    ).clip(lower=0)

    # Calculate all the seconds when the new clips shouldn't start
    uploaded_clips_df["seconds"] = [
        list(range(i, j + 1))
        for i, j in uploaded_clips_df[["clip_start_time", "clip_end_time"]].values
    ]

    # Reshape the dataframe of the seconds when the new clips shouldn't start
    uploaded_start = expand_list(uploaded_clips_df, "seconds", "upl_seconds")[
        ["movie_id", "upl_seconds"]
    ]

    # Exclude starting times of clips that have already been uploaded
    potential_clips_df = (
        pd.merge(
            potential_start_df,
            uploaded_start,
            how="left",
            left_on=["movie_id", "pot_seconds"],
            right_on=["movie_id", "upl_seconds"],
            indicator=True,
        )
        .query('_merge == "left_only"')
        .drop(columns=["_merge"])
    )
    
    # Combine the flatten metadata with the subjects df
    subj_df = pd.concat([subj_df, meta_df], axis=1)

    # Filter clip subjects
    subj_df = subj_df[subj_df['Subject_type']=="clip"]

    # Create a dictionary with the right types of columns
    subj_df = {
    'subject_id': subj_df['subject_id'].astype(int),
    'upl_seconds': subj_df['upl_seconds'].astype(int),
    'clip_length': subj_df['#clip_length'].astype(int),
    'VideoFilename': subj_df['#VideoFilename'].astype(str),
    }

    # Transform the dictionary created above into a new DataFrame
    subj_df = pd.DataFrame(subj_df)

    # Calculate all the seconds uploaded
    subj_df["seconds"] = [list(range(i, i+j, 1)) for i, j in subj_df[['upl_seconds','clip_length']].values]

    # Reshape the dataframe of potential seconds for the new clips to start
    subj_df = expand_list(subj_df, "seconds", "upl_seconds").drop(columns=['clip_length'])


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

def choose_subjectset_method():
    
    # Specify whether to upload to a new or existing workflow 
    subjectset_method = widgets.ToggleButtons(
        options=['Existing','New'],
        description='Subjectset destination:',
        disabled=False,
        button_style='success',
    )
    
    display(subjectset_method)
    return subjectset_method

def choose_subjectset(df, method):
    
    if method=="Existing":
        # Select subjectset availables
        subjectset = widgets.Combobox(
            options=list(df.subject_set_id.apply(str).unique()),
            description='Subjectset id:',
            ensure_option=True,
            disabled=False,
        )
    else:
        # Specify the name of the new subjectset
        subjectset = widgets.Text(
            placeholder='Type subjectset name',
            description='New subjectset name:',
            disabled=False
        )
        
    display(subjectset)
    return subjectset






# def choose_subject_set1(subjectset_df):
    
#     # Specify whether to upload to a new or existing workflow 
#     subjectset_method = widgets.ToggleButtons(
#         options=['Existing','New'],
#         description='Subjectset destination:',
#         disabled=False,
#         button_style='success',
#     )
#     output = widgets.Output()
    
#     def on_button_clicked(method):
#         with output:
#             if method['new']=="Existing":
#                 output.clear_output()
#                 # Select subjectset availables
#                 subjectset = widgets.Combobox(
#                     options=list(subjectset_df.subject_set_id.apply(str).unique()),
#                     description='Subjectset id:',
#                     ensure_option=True,
#                     disabled=False,
#                 )
                
#                 display(subjectset)

#                 return subjectset
                
#             else:
#                 output.clear_output()
#                 # Specify the name of the new subjectset
#                 subjectset = widgets.Text(
#                     placeholder='Type subjectset name',
#                     description='New subjectset name:',
#                     disabled=False
#                 )
            
            
#                 display(subjectset)

#                 return subjectset
            
            
#     subjectset_method.observe(on_button_clicked, names='value')
    
#     display(subjectset_method, output)

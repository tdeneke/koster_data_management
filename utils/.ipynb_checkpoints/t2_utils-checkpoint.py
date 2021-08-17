import pandas as pd
import numpy as np
import json, io
from ast import literal_eval
from utils import db_utils
from collections import OrderedDict
from IPython.display import HTML, display, update_display, clear_output
import ipywidgets as widgets

def upload_movies():
    
    # Define widget to upload the files
    mov_to_upload = widgets.FileUpload(
        accept='.mpg',  # Accepted file extension e.g. '.txt', '.pdf', 'image/*', 'image/*,.pdf'
        multiple=True  # True to accept multiple files upload else False
    )
    
    # Display the widget?
    display(mov_to_upload)
    
    main_out = widgets.Output()
    display(main_out)
    
    # TODO Copy the movie files to the movies folder
    
    
    print("uploaded")
    
    
    
def upload_movies():

    # Select the way to upload the info about the movies
    widgets.ToggleButton(
    value=False,
    description=['I have a csv file with information about the movies',
                 'I am happy to write here all the information about the movies'],
    disabled=False,
    button_style='success', # 'success', 'info', 'warning', 'danger' or ''
    tooltip='Description',
    icon='check'
)
    
    # Upload the information using a csv file
    widgets.FileUpload(
    accept='',  # Accepted file extension e.g. '.txt', '.pdf', 'image/*', 'image/*,.pdf'
    multiple=False  # True to accept multiple files upload else False
)
    # Upload the information 
    
    # the folder where the movies are
    
    # Try to extract location and date from the movies 
    widgets.DatePicker(
    description='Pick a Date',
    disabled=False
)
    
    # Run an interactive way to write metadata info about the movies
    
    print("Thanks for providing all the required information about the movies")
    
    
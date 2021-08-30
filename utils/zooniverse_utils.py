import io
import pandas as pd
import json
from panoptes_client import (
    SubjectSet,
    Subject,
    Project,
    Panoptes,
)

from utils.koster_utils import process_koster_subjects

class AuthenticationError(Exception):
    pass


# Function to authenticate to Zooniverse
def auth_session(username, password, project_n):
    
    # Connect to Zooniverse with your username and password
    auth = Panoptes.connect(username=username, password=password)

    if not auth.logged_in:
        raise AuthenticationError("Your credentials are invalid. Please try again.")

    # Specify the project number of the koster lab
    project = Project(project_n)

    return project 

# Function to retrieve information from Zooniverse
def retrieve_zoo_info(username: str, 
                      password: str,
                      project_n: str,
                      zoo_info: str):
    
    print("Connecting to the Zooniverse project")
    
    # Connect to the Zooniverse project
    project = auth_session(username, password, project_n)
    
    # Create an empty dictionary to host the dfs of interest
    info_df = {}
    
    for info_n in zoo_info:
        print("Retrieving", info_n, "from Zooniverse")
        
        # Get the information of interest from Zooniverse
        export = project.get_export(info_n)

        try:
            # Save the info as pandas data frame
            export_df = pd.read_csv(
                io.StringIO(export.content.decode("utf-8"))
            )
        
        except:
            raise ValueError("Request time out, please try again in 1 minute.")
        
        # Add df to dictionary
        info_df[info_n] = export_df
        
        print(info_n, "were retrieved succesfully")
    

    return info_df

# Function to extract metadata from subjects
def extract_metadata(subj_df):

    # Reset index of df
    subj_df = subj_df.reset_index(drop=True).reset_index()

    # Flatten the metadata information
    meta_df = pd.json_normalize(subj_df.metadata.apply(json.loads))

    # Drop metadata and index columns from original df
    subj_df = subj_df.drop(columns=["metadata", "index",])

    return subj_df, meta_df

def populate_subjects(subjects, project_n, db_path):
    
    #Check if the Zooniverse project is the KSO
    if str(project_n)=="9747":
        
        subjects = process_koster_subjects(subjects, db_path)
        
    else:
        # Extract metadata from uploaded subjects
        subjects_df, subjects_meta = extract_metadata(subjects)

        # Combine metadata info with the subjects df
        subjects = pd.concat([subjects_df, subjects_meta], axis=1)
        
    # Set subject_id information as id
    subjects = subjects.rename(columns={"subject_id": "id"})

    # Set the columns in the right order
    subjects = subjects[
        [
            "id",
            "subject_type",
            "filename",
            "clip_start_time",
            "clip_end_time",
            "frame_exp_sp_id",
            "frame_number",
            "workflow_id",
            "subject_set_id",
            "classifications_count",
            "retired_at",
            "retirement_reason",
            "created_at",
            "movie_id",
        ]
    ]

    # Ensure that subject_ids are not duplicated by workflow
    subjects = subjects.drop_duplicates(subset='id')
    
    # Test table validity
    db_utils.test_table(subjects, "subjects", keys=["movie_id"])

    # Add values to subjects
    db_utils.add_to_table(
        args.db_path, "subjects", [tuple(i) for i in subjects.values], 14
    )
    

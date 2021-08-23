import io, os
import getpass

from pathlib import Path
from utils.db_utils import download_init_csv
from init import init_db
from static import static_setup
from subjects_uploaded import retrieve_zooniverse_subjects

def main():

    # Define the path to the csv files with inital info to build the db
    db_csv_info = "../db_starter/db_csv_info/" 
    
    # Check if  the directory db_csv_info exists
    if not os.path.exists(db_csv_info):
        
        # Provide ID of the GDrive zipped folder with the init. database information
        gdrive_id = getpass.getpass('Enter the ID of the Google Drive zipped folder with the database information. \n The ID of the template information is: 1PZGRoSY_UpyLfMhRphMUMwDXw4yx1_Fn')
        
        # Download the csv files
        download_init_csv(gdrive_id, db_csv_info)
        
        
    # Define the path to the csv files with inital info to build the db
    for file in Path(db_csv_info).rglob("*.csv"):
        if 'sites' in file.name:
            sites_csv = file
        if 'movies' in file.name:
            movies_csv = file
        if 'species' in file.name:
            species_csv = file
        if 'duplicat' in file.name:
            duplicated_csv = file

    # Your user name and password for Zooniverse. 
    zoo_user = getpass.getpass('Enter your Zooniverse user')
    zoo_pass = getpass.getpass('Enter your Zooniverse password')
    
    # Initiate the sql db
    init_db()
    
    # Populate the db with initial info from csv files
    static_setup(sites_csv,movies_csv,species_csv)
    
    # Populate the db with subject info and deal with duplicates if any
    if duplicated_csv:
        retrieve_zooniverse_subjects(zoo_user, zoo_pass, duplicated_csv)
    else:
        retrieve_zooniverse_subjects(zoo_user, zoo_pass)
    
if __name__ == "__main__":
    main()

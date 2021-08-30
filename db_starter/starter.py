import os
import argparse

from pathlib import Path
from utils.db_utils import download_init_csv
from init import init_db
from static import static_setup

def main():
    
    "Handles argument parsing and launches the correct function."
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-mp",
        "--movies_path",
        type=str,
        help="the absolute path to the movie files",
        default=r"/uploads",
        required=False,
    )
    parser.add_argument(
        "-db",
        "--db_path",
        type=str,
        help="the absolute path to the database file",
        default=r"koster_lab.db",
        required=False,
    )

    args = parser.parse_args()
    
    # Define the path to the csv files with initial info to build the db
    db_csv_info = "../db_starter/db_csv_info/" 
    
    # Check if the directory db_csv_info exists
    if not os.path.exists(db_csv_info):
        
        print("There is no folder with initial information about the sites, movies and species.\n Please enter the ID of a Google Drive zipped folder with the inital database information. \n For example, the ID of the template information is: 1PZGRoSY_UpyLfMhRphMUMwDXw4yx1_Fn")
        
        # Provide ID of the GDrive zipped folder with the init. database information
        gdrive_id = getpass.getpass('ID of Google Drive zipped folder')
        
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
        

    # Initiate the sql db
    init_db(args.db_path)
    
    # Populate the db with initial info from csv files
    static_setup(sites_csv, movies_csv, species_csv, args.movies_path, args.db_path)
    
    
if __name__ == "__main__":
    main()

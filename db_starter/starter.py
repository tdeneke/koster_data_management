import argparse

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
    parser.add_argument(
        "-pj",
        "--project_name",
        type=str,
        help="the name of your project",
        default=r"Project example",
        required=False,
    )

    args = parser.parse_args()
          
    # Initiate the sql db
    init_db(args.db_path)
    
    # Populate the db with initial info from csv files
    static_setup(args.movies_path, args.project_name, args.db_path)
    
    
if __name__ == "__main__":
    main()

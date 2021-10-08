#spyfish utils
import sqlite3
import pandas as pd

import utils.db_utils as db_utils
import utils.server_utils as server_utils
from tqdm import tqdm
import subprocess

def add_movie_filenames(movies_df):

    #####Get info from bucket#####
    # Your acess key for the s3 bucket. 
    aws_access_key_id, aws_secret_access_key = server_utils.aws_credentials
    
    # Specify the bucket where the BUV files are
    bucket_i = movies_df['bucket'].str.split('/').str[0].unique().str[0]
    
    # Retrieve info from the bucket
    contents_s3_pd = server_utils.retrieve_s3_buckets_info(aws_access_key_id,aws_secret_access_key, bucket_i)

    # Include server's path to the movie files
    movies_df["Fpath"] = movies_path["bucket"] + "/" + movies_df["filename"]

    # Specify the 'key' or path to the BUV directories
    movies_df['key'] = movies_df['bucket'].str.split('/',1).str[1]

    # Specify the filename to be saved to
    movies_df['VideoFilename'] = movies_df['filename']

    # Select the relevant bucket
    bucket_i = movies_to_upload.bucket.unique()[0]
    objs = client.list_objects(Bucket=bucket_i)

    # Set the contents as pandas dataframe
    filenames_s3_buv_pd = pd.DataFrame(objs['Contents'])
    
    # Select only surveys that are missing filenames
    unprocessed_movies_df = movies_df[movies_df["filename"].isna()].reset_index(drop=True)
    
    # Write the filename of the concatenated movie
    unprocessed_movies_df["filename"] = unprocessed_movies_df["siteName"] + "_" + unprocessed_movies_df["created_on"].str.replace('/','_')+ ".MP4"
    
    # Download go pro videos, concatenate them and upload the concatenated videos to aws
    concatenate_videos(unprocessed_movies_df)

    
    # Add the updated movie info 
    movies_df = movies_df[~movies_df["filename"].isna()].reset_index(drop=True).append(unprocessed_movies_df)
    
    
    # Update the local movies.csv file with the new fps and duration information
    df.drop(["Fpath","exists"], axis=1).to_csv(movies_csv,index=False)
        
    ######Merge csv and bucket information
    # Check that videos can be mapped
    movies_df['exists'] = movies_df['Fpath'].map(os.path.isfile)
    
    # Create the folder to store the concatenated videos if not exist
    if not os.path.exists(concat_folder):
        os.mkdir(concat_folder)

    # Specify the prefixes of the BUV Go Pro files
    movies_to_upload["directory_prefix"] = movies_to_upload['key'] + "/GH"
    
    #movies_to_upload = apply(concatenate_go_pro x["VideoFilename","go_pro_files"])

# Function to download go pro videos, concatenate them and upload the concatenated videos to aws 
def concatenate_videos(df, session):

    # Loop through each survey to find out the raw videos recorded with the GoPros
    for index, row in tqdm(df.iterrows(), total=df.shape[0]):
        
        # Select the go pro videos from the "i" survey to concatenate
        bucket_1 = row['bucket'].split('/', 1)[1]
        list1 = row['go_pro_files'].split(';')
        list_go_pro = [bucket_1 + "/" + s for s in list1]

        # Start text file and list to keep track of the videos to concatenate
        textfile_name = "a_file.txt"
        textfile = open(textfile_name, "w")
        video_list = []

        print("Downloading", len(list_go_pro), "videos")

        # Download each go pro video from the S3 bucket
        for go_pro_i in tqdm(list_go_pro, total=len(list_go_pro)):
            
            print("Downloading", go_pro_i)
            
            # Specify the temporary output of the go pro file
            go_pro_output = go_pro_i.split("/")[-1]

            # Download the files from the S3 bucket
            if not os.path.exists(go_pro_output):
                server_utils.download_object_from_s3(
                    session,
                    bucket=bucket_i,
                    key=go_pro_i,
                    filename=go_pro_output,
                )
                
                #client.download_file(bucket_i, go_pro_i, go_pro_output)

            # Keep track of the videos to concatenate 
            textfile.write("file '"+ go_pro_output + "'"+ "\n")
            video_list.append(go_pro_output)

        textfile.close()

        concat_video = row['filename']

        if not os.path.exists(concat_video):

            print("Concatenating ",concat_video)

            # Concatenate the videos
            subprocess.call(["ffmpeg", 
                             "-f", "concat", 
                             "-safe", "0",
                             "-i", "a_file.txt", 
                             "-c", "copy", 
                             #"-an",#removes the audio
                             concat_video])
            
            print(concat_video, "concatenated successfully")

        # Upload the concatenated video to the S3
        #client.upload_file(concat_video, bucket_1, concat_video)
        s3_destination = bucket_1 + "/" + concat_video
        server_utils.upload_file_to_s3(
            session,
            bucket=bucket_i,
            key=s3_destination,
            filename=concat_video,
        )
                    
        print(concat_video, "succesfully uploaded to", s3_destination)
        
        # Delete the raw videos downloaded from the S3 bucket
        for f in video_list:
            os.remove(f)

        # Delete the text file
        os.remove(textfile_name)
        
        # Delete the text file
        os.remove(concat_video)

        print("Temporary files and videos removed")
       
    
def process_spyfish_subjects(subjects, db_path):
    
    # Merge "#Subject_type" and "Subject_type" columns to "subject_type"
    subjects['subject_type'] = subjects['Subject_type'].fillna(subjects['#Subject_type'])
    
    # Rename columns to match the db format
    subjects = subjects.rename(
        columns={
            "#VideoFilename": "filename",
            "upl_seconds": "clip_start_time",
            "#frame_number": "frame_number"
        }
    )
    
    # Calculate the clip_end_time
    subjects["clip_end_time"] = subjects["clip_start_time"] + subjects["#clip_length"] 
    
    # Create connection to db
    conn = db_utils.create_connection(db_path)
    
    ##### Match 'ScientificName' to species id and save as column "frame_exp_sp_id" 
    # Query id and sci. names from the species table
    species_df = pd.read_sql_query("SELECT id, scientificName FROM species", conn)
    
    # Rename columns to match subject df 
    species_df = species_df.rename(
        columns={
            "id": "frame_exp_sp_id",
            "scientificName": "ScientificName"
        }
    )
    
    # Reference the expected species on the uploaded subjects
    subjects = pd.merge(subjects, species_df, how="left", on="ScientificName")

    ##### Match site code to name from movies sql and get movie_id to save it as "movie_id"
    # Query id and filenames from the movies table
    movies_df = pd.read_sql_query("SELECT id, filename FROM movies", conn)
    
    # Rename columns to match subject df 
    movies_df = movies_df.rename(
        columns={
            "id": "movie_id"
        }
    )
    
    # Reference the movienames with the id movies table
    subjects = pd.merge(subjects, movies_df, how="left", on="filename")
    
    return subjects

        
def process_clips_spyfish(annotations, row_class_id, rows_list):
    
    for ann_i in annotations:
        if ann_i["task"] == "T0":
            # Select each species annotated and flatten the relevant answers
            for value_i in ann_i["value"]:
                choice_i = {}
                # If choice = 'nothing here', set follow-up answers to blank
                if value_i["choice"] == "NOTHINGHERE":
                    f_time = ""
                    inds = ""
                # If choice = species, flatten follow-up answers
                else:
                    answers = value_i["answers"]
                    for k in answers.keys():
                        if "EARLIESTPOINT" in k:
                            f_time = answers[k].replace("S", "")
                        if "HOWMANY" in k:
                            inds = answers[k]
                        elif "EARLIESTPOINT" not in k and "HOWMANY" not in k:
                            f_time, inds = None, None

                # Save the species of choice, class and subject id
                choice_i.update(
                    {
                        "classification_id": row_class_id,
                        "label": value_i["choice"],
                        "first_seen": f_time,
                        "how_many": inds,
                    }
                )

                rows_list.append(choice_i)
               
            
            
    return rows_list
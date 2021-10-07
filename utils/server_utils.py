import os, io
import requests
import pandas as pd
import numpy as np
import getpass
import gdown
import zipfile
import boto3

from tqdm import tqdm
from pathlib import Path

# Common utility functions to connect to external servers (AWS, GDrive,...)

def download_csv_from_google_drive(file_url):

    # Download the csv files stored in Google Drive with initial information about
    # the movies and the species

    file_id = file_url.split("/")[-2]
    dwn_url = "https://drive.google.com/uc?export=download&id=" + file_id
    url = requests.get(dwn_url).text.encode("ISO-8859-1").decode()
    csv_raw = io.StringIO(url)
    dfs = pd.read_csv(csv_raw)
    return dfs


def download_init_csv(gdrive_id, db_csv_info):
    
    # Specify the url of the file to download
    url_input = "https://drive.google.com/uc?id=" + str(gdrive_id)
    
    print("Retrieving the file from ", url_input)
    
    # Specify the output of the file
    zip_file = 'db_csv_info.zip'
    
    # Download the zip file
    gdown.download(url_input, zip_file, quiet=False)
    
    # Unzip the folder with the csv files
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(os.path.dirname(db_csv_info))
        
        
    # Remove the zipped file
    os.remove(zip_file)
    
    
def get_sites_movies_species():
    
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
            
            
    return sites_csv, movies_csv, species_csv


def aws_credentials():
    # Save your access key for the s3 bucket. 
    aws_access_key_id = getpass.getpass('Enter the key id for the aws server')
    aws_secret_access_key = getpass.getpass('Enter the secret access key for the aws server')
    
    return aws_access_key_id,aws_secret_access_key


def connect_s3(aws_access_key_id, aws_secret_access_key):
    # Connect to the s3 bucket
    client = boto3.client('s3',
                          aws_access_key_id = aws_access_key_id, 
                          aws_secret_access_key = aws_secret_access_key)
    return client


def retrieve_s3_buckets_info(client, bucket_i):

    # Select the relevant bucket
    objs = client.list_objects(Bucket=bucket_i)

    # Set the contents as pandas dataframe
    contents_s3_pd = pd.DataFrame(objs['Contents'])
    
    return contents_s3_pd


def download_object_from_s3(client, *, bucket, key, version_id=None, filename):
    """
    Download an object from S3 with a progress bar.

    From https://alexwlchan.net/2021/04/s3-progress-bars/
    """

    # First get the size, so we know what tqdm is counting up to.
    # Theoretically the size could change between this HeadObject and starting
    # to download the file, but this would only affect the progress bar.
    kwargs = {"Bucket": bucket, "Key": key}

    if version_id is not None:
        kwargs["VersionId"] = version_id

    object_size = client.head_object(**kwargs)["ContentLength"]

    if version_id is not None:
        ExtraArgs = {"VersionId": version_id}
    else:
        ExtraArgs = None

    with tqdm(total=object_size, unit="B", unit_scale=True, desc=filename, position=0, leave=True) as pbar:
        client.download_file(
            Bucket=bucket,
            Key=key,
            ExtraArgs=ExtraArgs,
            Filename=filename,
            Callback=lambda bytes_transferred: pbar.update(bytes_transferred),
        )


def upload_file_to_s3(client, *, bucket, key, filename):
    
    # Get the size of the file to upload
    file_size = os.stat(filename).st_size

    with tqdm(total=file_size, unit="B", unit_scale=True, desc=filename, position=0, leave=True) as pbar:
        client.upload_file(
            Filename=filename,
            Bucket=bucket,
            Key=key,
            Callback=lambda bytes_transferred: pbar.update(bytes_transferred),
        )

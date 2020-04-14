import os, math, csv
import subprocess
import argparse
import sqlite3
import numpy as np
from db_setup import *
from zooniverse_setup import *
from datetime import date
from panoptes_client import (
    SubjectSet,
    Subject,
    Project,
    Panoptes,
)  # needed to upload clips to Zooniverse


def get_length(filename):
    result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            filename,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    return float(result.stdout)


def main():

    "Handles argument parsing and launches the correct function."
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--user", "-u", help="Zooniverse username", type=str, required=True
    )
    parser.add_argument(
        "--password", "-p", help="Zooniverse password", type=str, required=True
    )
    parser.add_argument(
        "--location", "-l", help="Location to store clips", type=str, required=True
    )
    parser.add_argument(
        "--video_path", "-v", help="Video to clip", type=str, required=True
    )
    parser.add_argument(
        "--n_clips", "-n", help="Number of clips to sample", type=str, required=True
    )
    parser.add_argument(
        "-db",
        "--db_path",
        type=str,
        help="the absolute path to the database file",
        default=r"koster_lab.db",
        required=True,
    )
    args = parser.parse_args()

    # Connect to koster_db
    conn = create_connection(args.db_path)

    # video_filename = str(os.path.basename(args.video_path))
    # video_duration = get_length(args.video_path)
    # v_filename, ext = os.path.splitext(video_filename)

    # Choose a movie in the db to clip
    try:
        movie_id = retrieve_query(
            conn, "SELECT id FROM movies EXCEPT SELECT movie_id from clips"
        )[0][0]
        v_filename = retrieve_query(
            conn, f"SELECT filename FROM movies WHERE id=={movie_id}"
        )[0][0]
    except:
        raise AssertionError("No such movie exists")

    # Specify how many clips to generate and its length (seconds)
    n_clips = args.n_clips
    clip_length = 10
    video_duration = 250  # get_length(filename)
    # Only for testing
    # video_path = "../../../data/videos/test_video.mp4"

    # Split movie into clips of fixed duration

    # Specify the folder location to store the clips
    location = args.location
    if not os.path.exists(location):
        os.mkdir(location)

    # Specify the project number of the koster lab
    koster_project = auth_session(args.user, args.password)

    # Create empty subject metadata to keep track of the clips generated
    subject_metadata = {}

    # Generate one clip at the time, update the koster lab database and the subject_metadata
    for clip in np.arange(0, video_duration, clip_length):
        # Generate and store the clip
        subject_filename = v_filename + "_" + str(int(clip)) + ".mp4"
        fileoutput = location + os.sep + subject_filename
        subprocess.call(
            [
                "ffmpeg",
                "-ss",
                str(clip),
                "-t",
                str(clip_length),
                "-i",
                video_path,
                "-c",
                "copy",
                "-force_key_frames",
                "1",
                fileoutput,
            ]
        )

        # Add clip information to the koster lab database
        # clip_id =
        filename = subject_filename
        start_time = clip
        end_time = clip + clip_length
        clip_date = date.today().strftime("%d_%m_%Y")

        # Add clip information to the subject_metadata
        subject_metadata[clip] = {
            "filename": subject_filename,
            "#start_time": start_time,
            "#end_time": end_time,
            "clip_date": clip_date,
        }

    print(
        len(np.arange(0, video_duration, clip_length)),
        " clips have been generated in ",
        location,
        ".",
    )

    # Create a new subject set (the Zooniverse dataset that will store the clips)
    set_name = input("clips_" + date.today().strftime("%d_%m_%Y"))
    previous_subjects = []

    try:
        # check if the subject set already exits
        subject_set = SubjectSet.where(
            project_id=koster_project.id, display_name=set_name
        ).next()
        print(
            "You have chosen to upload ",
            len(subject_metadata),
            " files to an existing subject set",
            set_name,
        )
        retry = input(
            'Enter "n" to cancel this upload, any other key to continue' + "\n"
        )
        if retry.lower() == "n":
            quit()
        for subject in subject_set.subjects:
            previous_subjects.append(subject.metadata["filename"])
    except StopIteration:
        print(
            "You have chosen to upload ",
            len(subject_metadata),
            " files to an new subject set ",
            set_name,
        )
        retry = input(
            'Enter "n" to cancel this upload, any other key to continue' + "\n"
        )
        if retry.lower() == "n":
            quit()
        # create a new subject set for the new data and link it to the project above
        subject_set = SubjectSet()
        subject_set.links.project = koster_project
        subject_set.display_name = set_name
        subject_set.save()

    # Upload the clips to the project
    print("Uploading subjects, this could take a while!")
    new_subjects = 0

    for filename, metadata in subject_metadata.items():
        try:
            if filename not in previous_subjects:
                subject = Subject()
                subject.links.project = koster_project
                subject.add_location(location + os.sep + filename)
                subject.metadata.update(metadata)
                subject.save()
                subject_set.add(subject.id)
                print(filename)
                new_subjects += 1
        except panoptes_client.panoptes.PanoptesAPIException:
            print("An error occurred during the upload of ", filename)
    print(new_subjects, "new subjects created and uploaded")

    # test the validity of table entries

    test_table(clips, "clips")
    test_table(subjects, "subjects")

    # update the tables (check these values)

    try:
        insert_many(conn, [tuple(i) for i in clips.values], "clips", 6)
        insert_many(conn, [tuple(i) for i in subjects.values], "subjects", 8)
    except sqlite3.Error as e:
        print(e)

    conn.commit()

    # Generate a csv file with all the uploaded clips
    uploaded = 0
    with open(location + os.sep + "Uploaded subjects.csv", "wt") as file:
        subject_set = SubjectSet.where(
            project_id=koster_project.id, display_name=set_name
        ).next()
        for subject in subject_set.subjects:
            uploaded += 1
            file.write(subject.id + "," + list(subject.metadata.values())[0] + "\n")

        print(
            uploaded,
            " subjects found in the subject set, see the full list in Uploaded subjects.csv.",
        )


if __name__ == "__main__":
    main()

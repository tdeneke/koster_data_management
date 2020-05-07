import io, os, csv, json, sys, re
import operator, argparse
import requests
import pandas as pd
from datetime import datetime
from db_setup import *
from zooniverse_setup import *

# Specify the workflow of interest and its version
workflow_1 = 11767
workflow_1_version = 227


def get_id(conn, row):

    try:
        gid = retrieve_query(
            conn, f"SELECT clip_id FROM subjects WHERE id=={int(row['subject_id'])}",
        )[0][0]
    except:
        gid = None
    return gid


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
        "-db",
        "--db_path",
        type=str,
        help="the absolute path to the database file",
        default=r"koster_lab.db",
        required=True,
    )
    args = parser.parse_args()

    project = auth_session(args.user, args.password)

    # Get the export classifications
    export = project.get_export("classifications")

    # Save the response as pandas data frame
    rawdata = pd.read_csv(
        io.StringIO(export.content.decode("utf-8")),
        usecols=[
            "subject_ids",
            "classification_id",
            "workflow_id",
            "workflow_version",
            "annotations",
        ],
    )
    # Filter w1 classifications
    w1_data = rawdata[
        (rawdata.workflow_id >= workflow_1)
        & (rawdata.workflow_version >= workflow_1_version)
    ].reset_index()

    # Drop worflow columns
    w1_data = w1_data.drop(columns=["workflow_id", "workflow_version"])

    # Create an empty list 
    rows_list = []

    # loop through each classification submitted by the users
    for index, row in w1_data.iterrows():
        # load annotations as json format
        annotations = json.loads(row["annotations"])

        # Select the information from the species identification task
        for ann_i in annotations:
            if ann_i["task"] == "T4":
                
                # Select each species annotation and flatten the relevant answers
                for value_i in ann_i["value"]:
                    choice_i = {}
                    # If choice = 'nothing here', set follow up answers to blank
                    if value_i["choice"] == "NOTHINGHERE":
                        f_time = ""
                        inds = ""
                    # If choice = species, flatten the follow up answers
                    else:
                        answers = value_i["answers"]
                        for k in answers.keys():
                            if "FIRSTTIME" in k:
                                f_time = answers[k].replace("S", "")
                            if "INDIVIDUAL" in k:
                                inds = answers[k]
                                

                    # Create a new row with the species of choice, class and subject ids
                    choice_i.update(
                        {
                            "classification_id": row["classification_id"],
                            "label": value_i["choice"],
                            "first_seen": f_time,
                            "how_many": inds,
                        }
                    ) 
                    
                    # Include the new row in the list
                    rows_list.append(choice_i)

    #Create a data frame from the list of dictionaries 
    flat_data = pd.DataFrame(rows_list, 
                             columns=["classification_id", "label", "first_seen", "how_many"])

    # Specify the type of columns
    flat_data["how_many"] = pd.to_numeric(flat_data["how_many"])
    flat_data["first_seen"] = pd.to_numeric(flat_data["first_seen"])

    # Add the subject_ids to the dataframe
    class_data = pd.merge(
        flat_data,
        w1_data.drop(columns=["annotations"]),
        how="left",
        on="classification_id",
    )

    # Calculate the number of different classifications per subject
    class_data["class_subject"] = class_data.groupby("subject_ids")[
        "classification_id"
    ].transform("nunique")

    # Select subjects with at least 4 different classifications
    class_data = class_data[class_data.class_subject > 3]

    # Calculate the proportion of users that agreed on their classifications
    class_data["class_n"] = class_data.groupby(["subject_ids", "label"])[
        "classification_id"
    ].transform("count")
    class_data["class_prop"] = class_data.class_n / class_data.class_subject

    # Select subjects where at least 80% of the users agree in their classification
    class_data = class_data[class_data.class_prop > 0.8]

    # extract the median of the second where the animal/object is and the number of animals
    class_data = class_data.groupby(["subject_ids", "label"], as_index=False)
    class_data = pd.DataFrame(class_data[["how_many", "first_seen"]].median())

    # add index as id
    class_data = class_data.reset_index().rename(
        columns={"index": "id", "subject_ids": "subject_id"}
    )

    # create connection to db
    conn = create_connection(args.db_path)

    # Retrieve the id and clip_id from the subjects table
    subjects_df = pd.read_sql_query("SELECT id, clip_id FROM subjects", conn)
    subjects_df = subjects_df.rename(columns={"id": "subject_id"})

    # Reference with subjects table
    class_data = pd.merge(
        class_data, subjects_df, how="left", on="subject_id", validate="many_to_one"
    )

    # Retrieve the id and label from the species table
    speciesdf = pd.read_sql_query("SELECT id, label FROM species", conn)
    speciesdf = speciesdf.rename(columns={"id": "species_id"})

    # Match the label format of speciesdf to the class_data
    speciesdf["label"] = speciesdf["label"].apply(
        lambda x: re.sub(r"[()\s]", "", x).upper(), 1
    )

    # add species_id to the classifications dataframe
    class_data = pd.merge(
        class_data, speciesdf, how="left", on="label", validate="many_to_one"
    )

    # Select information to include in the agg_annotations table
    class_data = class_data[["id", "species_id", "how_many", "first_seen", "clip_id"]]

    # Add to agg_annotations_clip table
    try:
        insert_many(
            conn, [tuple(i) for i in class_data.values], "agg_annotations_clip", 5
        )
    except sqlite3.Error as e:
        print(e)

    conn.commit()

    print("Aggregation complete")


if __name__ == "__main__":
    main()

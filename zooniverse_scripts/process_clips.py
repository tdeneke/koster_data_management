import io, os, csv, json, sys, re
import operator, argparse
import requests, db_utils
import pandas as pd
from datetime import datetime
from zooniverse_setup import auth_session

# Specify the workflow of interest and its version
workflow_clip = 11767
workflow_clip_version = 227


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

    # Connect to the Zooniverse project
    project = auth_session(args.user, args.password)

    # Get the classifications from the project
    export = project.get_export("classifications")

    # Save the response as pandas data frame
    class_df = pd.read_csv(
        io.StringIO(export.content.decode("utf-8")),
        usecols=[
            "subject_ids",
            "classification_id",
            "workflow_id",
            "workflow_version",
            "annotations",
        ],
    )
    
    # Filter clip classifications
    class_df = class_df[
        (class_df.workflow_id >= workflow_clip)
        & (class_df.workflow_version >= workflow_clip_version)
    ].reset_index()

    # Drop worflow columns
    class_df = class_df.drop(columns=["workflow_id", "workflow_version"])

    # Create an empty list 
    rows_list = []

    # Loop through each classification submitted by the users
    for index, row in class_df.iterrows():
        # Load annotations as json format
        annotations = json.loads(row["annotations"])

        # Select the information from the species identification task
        for ann_i in annotations:
            if ann_i["task"] == "T4":
                
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
                            if "FIRSTTIME" in k:
                                f_time = answers[k].replace("S", "")
                            if "INDIVIDUAL" in k:
                                inds = answers[k]
                                

                    # Save the species of choice, class and subject id
                    choice_i.update(
                        {
                            "classification_id": row["classification_id"],
                            "label": value_i["choice"],
                            "first_seen": f_time,
                            "how_many": inds,
                        }
                    ) 
                    
                    rows_list.append(choice_i)

    # Create a data frame with annotations as rows 
    annot_df = pd.DataFrame(rows_list, 
                             columns=["classification_id", "label", "first_seen", "how_many"])

    # Specify the type of columns of the df
    annot_df["how_many"] = pd.to_numeric(annot_df["how_many"])
    annot_df["first_seen"] = pd.to_numeric(annot_df["first_seen"])

    # Add subject id to each annotation
    annot_df = pd.merge(
        annot_df,
        class_df.drop(columns=["annotations"]),
        how="left",
        on="classification_id",
    )

    # Calculate the number of users that classified each subject
    annot_df["class_subject"] = annot_df.groupby("subject_ids")[
        "classification_id"
    ].transform("nunique")

    # Select subjects with at least 3 different user classifications
    annot_df = annot_df[annot_df.class_subject > 2]

    # Calculate the proportion of users that agreed on their annotations
    annot_df["class_n"] = annot_df.groupby(["subject_ids", "label"])[
        "classification_id"
    ].transform("count")
    annot_df["class_prop"] = annot_df.class_n / annot_df.class_subject

    # Select annotations where at least 80% of the users have agreed
    annot_df = annot_df[annot_df.class_prop > 0.79]

    # Extract the median of the second where the animal/object is and number of animals
    annot_df = annot_df.groupby(["subject_ids", "label"], as_index=False)
    annot_df = pd.DataFrame(annot_df[["how_many", "first_seen"]].median())

    # Create connection to db
    conn = db_utils.create_connection(args.db_path)

    # Retrieve the id and label from the species table
    speciesdf = pd.read_sql_query("SELECT id, label FROM species", conn)
    speciesdf = speciesdf.rename(columns={"id": "species_id"})

    # Match the label format of speciesdf to the annot_df
    speciesdf["label"] = speciesdf["label"].apply(
        lambda x: re.sub(r"[()\s]", "", x).upper(), 1
    )

    # Add species_id to the classifications dataframe
    annot_df = pd.merge(
        annot_df, speciesdf, how="left", on="label", validate="many_to_one"
    )
    
    # Add index as id and rename the subject_id field
    annot_df = annot_df.reset_index().rename(
        columns={"index": "id", "subject_ids": "subject_id"}
    )

    # Set the columns in the right order
    annot_df = annot_df[
        [
            "id", 
            "species_id", 
            "how_many", 
            "first_seen", 
            "subject_id",
        ]
    ]

    # Add annotations to the agg_annotations_clip table
    db_utils.add_to_table(
        args.db_path, "agg_annotations_clip", [tuple(i) for i in annot_df.values], 5
    )


if __name__ == "__main__":
    main()

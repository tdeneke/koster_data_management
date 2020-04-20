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

    # Currently we discard sites that have no lat or lon coordinates, since site descriptions are not unique
    # it becomes difficult to match this information otherwise
    try:
        gid = retrieve_query(
            conn,
            f"SELECT clip_id FROM subjects WHERE id=={int(row['subject_id'])}",
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

    # get the export classifications
    export = project.get_export("classifications")

    # save the response as pandas data frame
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

    # Create empty df
    flat_data = pd.DataFrame(
        columns=["classification_id", "label", "first_seen", "how_many"]
    )

    for index, row in w1_data.iterrows():
        # load annotations as json format
        annotations = json.loads(row["annotations"])

        # select the information from the species identification task
        for task_i in annotations:
            try:
                if task_i["task"] == "T4":
                    # select each species annotation and flatten the relevant answers
                    for species in task_i["value"]:
                        try:
                            # loop through the answers and add them to the row
                            answers = species["answers"]
                            if len(answers) == 0:
                                f_time = ""
                                inds = ""
                            else:
                                for k in answers.keys():
                                    try:
                                        if "FIRSTTIME" in k:
                                            f_time = answers[k].replace("S", "")
                                        if "INDIVIDUAL" in k:
                                            inds = answers[k]
                                    except KeyError:
                                        continue

                            # include a new row with the species of choice, class and subject ids
                            flat_data = flat_data.append(
                                {
                                    "classification_id": row["classification_id"],
                                    "label": species["choice"],
                                    "first_seen": f_time,
                                    "how_many": inds,
                                },
                                ignore_index=True,
                            )
                        except KeyError:
                            continue
            except KeyError:
                continue

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
    # Retrieve the id and clip_id from the subjects table

    #print(retrieve_query(create_connection(args.db_path), "SELECT * FROM subjects"))

    class_data["clip_id"] = class_data.apply(lambda x: get_id(create_connection(args.db_path), x), 1)

    # add clip_id to the classifications dataframe
    # class_data = pd.merge(
    #    class_data, subjects, how="left", left_on="subject_id", right_on="id"
    # )

    # Retrieve the id and label from the species table
    species = pd.DataFrame(
        retrieve_query(create_connection(args.db_path), "SELECT * FROM species"),
        columns=["species_id", "label"],
    )

    species['label'] = species['label'].apply(lambda x: re.sub(r'[()\s]', '', x).upper(), 1)

    # add species_id to the classifications dataframe
    class_data = pd.merge(
        class_data, species, how="left", left_on="label", right_on="label"
    )

    class_data = class_data[["id", "species_id", "how_many", "first_seen", "clip_id"]]
    # Add to agg_annotations_clip table
    conn = create_connection(args.db_path)

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

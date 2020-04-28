import os
import csv
import requests
from datetime import datetime
from panoptes_client import Project, Panoptes
from db_setup import *
from zooniverse_setup import *

# Specify the workflow of interest and its version
workflow_2 = 12852
workflow_2_version = 001.01

def get_species_id(conn, row):

    # Currently we discard sites that have no lat or lon coordinates, since site descriptions are not unique
    # it becomes difficult to match this information otherwise

    try:
        gid = retrieve_query(conn, f"SELECT id FROM species WHERE label=='{row['class_name']}'")[0][0]
    except IndexError:
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
            "subject_data",
            "classification_id",
            "workflow_id",
            "workflow_version",
            "annotations",
        ],
    )
    # Filter w2 classifications
    w2_data = rawdata[
        (rawdata.workflow_id >= workflow_2)
        & (rawdata.workflow_version >= workflow_2_version)
    ].reset_index()

    # Drop workflow columns
    w2_data = w2_data.drop(columns=["workflow_id", "workflow_version"])
    
    # Extract the video filename and annotation details
    w2_data["annotation"] = w2_data.apply(
        lambda x: (
            [v["filename"] for k, v in json.loads(x.subject_data).items()],
            literal_eval(x["annotations"])[0]["value"],
        )
        if len(literal_eval(x["annotations"])[0]["value"]) > 0
        else None,
        1,
    )

    # Convert annotation to format which the tracker expects
    ds = [
        OrderedDict(
            {
                "filename": i[0][0].split("_frame", 1)[0],
                "class_name": i[1][0]["tool_label"],
                "start_frame": int(i[0][0].split("_frame", 1)[1].replace(".jpg", "")),
                "x": int(i[1][0]["x"]),
                "y": int(i[1][0]["y"]),
                "w": int(i[1][0]["width"]),
                "h": int(i[1][0]["height"]),
            }
        )
        for i in df.annotation
        if i is not None
    ]
    
    # Get prepared annotations
    w2_annotations = pd.DataFrame(ds)

    # Get species id for each species
   	conn = create_connection(args.db_path)

    w2_annotations['species_id'] = w2_annotations.apply(lambda x: get_species_id(conn, x), 1)

    w2_annotations = w2_annotations.drop(columns="class_name")[["species_id", "x", "y", "w", "h", "frame"]]
    
    # Add frames to db
   	try:
        insert_many(conn, [tuple(i) for i in w2_annotations.values], "agg_annotations_frame", 4)
    except sqlite3.Error as e:
        print(e)

    conn.commit()


if __name__ == '__main__':
	main()
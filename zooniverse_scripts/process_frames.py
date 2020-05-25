import os, io, csv, json
import requests, db_utils, argparse
import pandas as pd
import numpy as np
from ast import literal_eval
from datetime import datetime
from panoptes_client import Project, Panoptes
from zooniverse_setup import auth_session
from collections import OrderedDict, Counter
from sklearn.cluster import DBSCAN

# Specify the workflow of interest and its version
workflow_2 = 12852
workflow_2_version = 001.01


def bb_iou(boxA, boxB):

    # Compute edges
    temp_boxA = boxA.copy()
    temp_boxB = boxB.copy()
    temp_boxA[2], temp_boxA[3] = (
        temp_boxA[0] + temp_boxA[2],
        temp_boxA[1] + temp_boxA[3],
    )
    temp_boxB[2], temp_boxB[3] = (
        temp_boxB[0] + temp_boxB[2],
        temp_boxB[1] + temp_boxB[3],
    )

    # determine the (x, y)-coordinates of the intersection rectangle
    xA = max(temp_boxA[0], temp_boxB[0])
    yA = max(temp_boxA[1], temp_boxB[1])
    xB = min(temp_boxA[2], temp_boxB[2])
    yB = min(temp_boxA[3], temp_boxB[3])

    # compute the area of intersection rectangle
    interArea = abs(max((xB - xA, 0)) * max((yB - yA), 0))
    if interArea == 0:
        return 1
    # compute the area of both the prediction and ground-truth
    # rectangles
    boxAArea = abs((temp_boxA[2] - temp_boxA[0]) * (temp_boxA[3] - temp_boxA[1]))
    boxBArea = abs((temp_boxB[2] - temp_boxB[0]) * (temp_boxB[3] - temp_boxB[1]))

    # compute the intersection over union by taking the intersection
    # area and dividing it by the sum of prediction + ground-truth
    # areas - the intersection area
    iou = interArea / float(boxAArea + boxBArea - interArea)

    # return the intersection over union value

    return 1 - iou


def filter_bboxes(total_users, users, bboxes):

    # If at least half of those who saw this frame decided that there was an object
    user_count = pd.Series(users).nunique()
    if user_count / total_users >= 0.5:
        # Get clusters of annotation boxes based on iou criterion
        cluster_ids = DBSCAN(min_samples=1, metric=bb_iou).fit_predict(bboxes)
        # Count the number of users within each cluster
        counter_dict = Counter(cluster_ids)
        # Accept a cluster assignment if at least 80% of users agree on annotation
        passing_ids = [k for k, v in counter_dict.items() if v / user_count >= 0.8]

        indices = np.isin(cluster_ids, passing_ids)

        final_boxes = []
        for i in passing_ids:
            # Compute median over all accepted bounding boxes
            boxes = np.array(bboxes)[np.where(cluster_ids == i)].median(axis=0)
            final_boxes.append(boxes)

        return indices, final_boxes

    else:
        return [], bboxes


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
            "user_name",
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
    w2_data["filename"] = w2_data["subject_data"].apply(
        lambda x: [{"filename": v["filename"]} for k, v in json.loads(x).items()][0]
    )
    w2_data["user_name"] = w2_data["user_name"].apply(lambda x: {"user_name": x})
    w2_data["subject_id"] = w2_data["subject_ids"].apply(lambda x: {"subject_id": x})
    w2_data["annotation"] = w2_data["annotations"].apply(
        lambda x: literal_eval(x)[0]["value"], 1
    )
    w2_data["annotation"] = w2_data[
        ["filename", "annotation", "user_name", "subject_id"]
    ].apply(
        lambda x: [
            OrderedDict(
                list(x["annotation"][i].items())
                + list(x["filename"].items())
                + list(x["user_name"].items())
                + list(x["subject_id"].items())
            )
            for i in range(len(x["annotation"]))
        ]
        if len(x["annotation"]) > 0
        else [OrderedDict(list(x["filename"].items()) + list(x["user_name"].items()))],
        1,
    )

    # Convert annotation to format which the tracker expects
    ds = [
        OrderedDict(
            {
                "user": i["user_name"],
                "filename": i["filename"].split("_frame", 1)[0],
                "class_name": i["tool_label"],
                "start_frame": int(
                    i["filename"].split("_frame", 1)[1].replace(".jpg", "")
                ),
                "x": int(i["x"]) if "x" in i else None,
                "y": int(i["y"]) if "y" in i else None,
                "w": int(i["width"]) if "width" in i else None,
                "h": int(i["height"]) if "height" in i else None,
                "subject_id": i["subject_id"] if "subject_id" in i else None,
            }
        )
        for i in w2_data.annotation.explode()
        if i is not None and i is not np.nan
    ]

    # Get prepared annotations
    w2_full = pd.DataFrame(ds)
    w2_annotations = w2_full[w2_full["class_name"].notnull()]
    new_rows = []
    final_indices = []
    for name, group in w2_annotations.groupby(
        ["filename", "class_name", "start_frame"]
    ):

        filename, class_name, start_frame = name

        total_users = w2_full[w2_full.filename == filename]["user_name"].nunique()

        # Filter bboxes using IOU metric (essentially a consensus metric)
        # Keep only bboxes where mean overlap exceeds this threshold
        indices, new_group = filter_bboxes(
            total_users=total_users,
            users=[i[0] for i in group.values],
            bboxes=[np.array((i[4], i[5], i[6], i[7])) for i in group.values],
        )

        subject_ids = [i[8] for i in group.values[indices]]

        for ix, box in zip(subject_ids, new_group):
            new_rows.append((filename, class_name, start_frame, ix,) + tuple(box))

    w2_annotations = pd.DataFrame(
        new_rows,
        columns=[
            "filename",
            "class_name",
            "start_frame",
            "subject_id",
            "x",
            "y",
            "w",
            "h",
        ],
    )

    # Get species id for each species
    conn = db_utils.create_connection(args.db_path)

    # Get subject table

    subjects_df = pd.read_sql_query(
        "SELECT id, frame_exp_sp_id, movie_id FROM subjects", conn
    )
    subjects_df = subjects_df.rename(
        columns={"id": "subject_id", "frame_exp_sp_id": "species_id"}
    )

    w2_annotations = pd.merge(
        w2_annotations,
        subjects_df,
        how="left",
        left_on="subject_id",
        right_on="subject_id",
        validate="many_to_one",
    )

    w2_annotations = w2_annotations[w2_annotations.movie_id.notnull()][
        ["species_id", "x", "y", "w", "h", "start_frame", "subject_id"]
    ]

    # Add values to agg_annotations_frame
    db_utils.add_to_table(
        args.db_path,
        "agg_annotations_frame",
        [(None,) + tuple(i) for i in w2_annotations.values],
        7,
    )

    print(f"Frame Aggregation Complete: {len(w2_annotations)} frames added")


if __name__ == "__main__":
    main()

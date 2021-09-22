import pandas as pd
import numpy as np
import json, io
from ast import literal_eval
from utils.zooniverse_utils import auth_session
import utils.db_utils as db_utils
from utils.koster_utils import filter_bboxes
from utils import db_utils
from collections import OrderedDict, Counter
from IPython.display import HTML, display, update_display, clear_output
import ipywidgets as widgets
from ipywidgets import interact
import asyncio


def choose_project():
    
    # Specify location of the latest list of projects
    projects_csv = "../db_starter/projects_list.csv" 
    
    # Read the latest list of projects
    projects_df = pd.read_csv(projects_csv)
    
    # Display the project options
    choose_project = widgets.Dropdown(
        options=projects_df.Project_name.unique().tolist(),
        value=projects_df.Project_name.unique().tolist()[0],
        description="Project:",
        disabled=False,
    )
    
    display(choose_project)
    
    return choose_project

    
def choose_agg_parameters(subject_type: str):
    agg_users = widgets.FloatSlider(
        value=0.8,
        min=0,
        max=1.0,
        step=0.1,
        description="Aggregation threshold:",
        disabled=False,
        continuous_update=False,
        orientation="horizontal",
        readout=True,
        readout_format=".1f",
        display="flex",
        flex_flow="column",
        align_items="stretch",
        style={"description_width": "initial"},
    )
    display(agg_users)
    min_users = widgets.IntSlider(
        value=3,
        min=1,
        max=20,
        step=1,
        description="Min numbers of users:",
        disabled=False,
        continuous_update=False,
        orientation="horizontal",
        readout=True,
        readout_format="d",
        display="flex",
        flex_flow="column",
        align_items="stretch",
        style={"description_width": "initial"},
    )
    display(min_users)
    if subject_type == "frame":
        agg_obj = widgets.FloatSlider(
            value=0.8,
            min=0,
            max=1.0,
            step=0.1,
            description="Object threshold:",
            disabled=False,
            continuous_update=False,
            orientation="horizontal",
            readout=True,
            readout_format=".1f",
            display="flex",
            flex_flow="column",
            align_items="stretch",
            style={"description_width": "initial"},
        )
        display(agg_obj)
        agg_iou = widgets.FloatSlider(
            value=0.5,
            min=0,
            max=1.0,
            step=0.1,
            description="IOU Epsilon:",
            disabled=False,
            continuous_update=False,
            orientation="horizontal",
            readout=True,
            readout_format=".1f",
            display="flex",
            flex_flow="column",
            align_items="stretch",
            style={"description_width": "initial"},
        )
        display(agg_iou)
        agg_iua = widgets.FloatSlider(
            value=0.8,
            min=0,
            max=1.0,
            step=0.1,
            description="Inter user agreement:",
            disabled=False,
            continuous_update=False,
            orientation="horizontal",
            readout=True,
            readout_format=".1f",
            display="flex",
            flex_flow="column",
            align_items="stretch",
            style={"description_width": "initial"},
        )
        display(agg_iua)
        return agg_users, min_users, agg_obj, agg_iou, agg_iua
    else:
        return agg_users, min_users


def choose_workflows(workflows_df):

    layout = widgets.Layout(width="auto", height="40px")  # set width and height

    # TODO display the workflow ids and versions
    w1 = widgets.Dropdown(
        options=workflows_df.display_name.unique().tolist(),
        value=workflows_df.display_name.unique().tolist()[0],
        description="Workflow name:",
        disabled=False,
    )

    w2 = widgets.Dropdown(
        options=list(map(float, workflows_df.version.unique().tolist())),
        value=float(workflows_df.version.unique().tolist()[0]),
        description="Minimum workflow version:",
        disabled=False,
        display="flex",
        flex_flow="column",
        align_items="stretch",
        style={"description_width": "initial"},
    )

    w3 = widgets.Dropdown(
        options=["frame", "clip"],
        value="clip",
        description="Subject type:",
        disabled=False,
    )

    # w1.observe(on_change)
    display(w1)
    # w2.observe(on_change)
    display(w2)
    # w3.observe(on_change)
    display(w3)

    return w1, w2, w3


def get_classifications(
    workflow_id: int, workflow_version: float, subj_type, class_df, subjects_df
):

    # Filter classifications of interest
    class_df = class_df[
        (class_df.workflow_id == workflow_id)
        & (class_df.workflow_version >= workflow_version)
    ].reset_index(drop=True)

    # Add information about the subject type
    try:
        class_df["subject_type"] = class_df["subject_data"].apply(
            lambda x: [v["subject_type"] for k, v in json.loads(x).items()][0]
        )

        # Ensure only classifications of one type of subject get analysed (frame or video)
        class_df = class_df[class_df.subject_type == subj_type]
    except:
        pass

    # Add information on the location of the subject
    total_df = pd.merge(
        class_df,
        subjects_df[["subject_id", "workflow_id", "locations"]],
        left_on=["subject_ids", "workflow_id"],
        right_on=["subject_id", "workflow_id"],
    )

    total_df["locations"] = total_df["locations"].apply(lambda x: literal_eval(x)["0"])

    print("Zooniverse classifications have been retrieved")

    return total_df, class_df


def aggregrate_classifications(df, subj_type, subjects, agg_params):

    print("Aggregrating the classifications")
    if subj_type == "frame":
        agg_users, min_users, agg_obj, agg_iou, agg_iua = [i.value for i in agg_params]
    else:
        agg_users, min_users = [i.value for i in agg_params]

    # Process the classifications of clips or frames
    if subj_type == "clip":
        raw_class_df = process_clips(df, subjects)

    if subj_type == "frame":
        raw_class_df = process_frames(df, subjects)

    # Calculate the number of users that classified each subject
    raw_class_df["n_users"] = raw_class_df.groupby("subject_ids")[
        "classification_id"
    ].transform("nunique")

    # Select classifications with at least n different user classifications
    raw_class_df = raw_class_df[raw_class_df.n_users >= min_users]

    # Calculate the proportion of users that agreed on their annotations
    raw_class_df["class_n"] = raw_class_df.groupby(["subject_ids", "label"])[
        "classification_id"
    ].transform("count")
    raw_class_df["class_prop"] = raw_class_df.class_n / raw_class_df.n_users

    # Select annotations based on agreement threshold
    raw_class_df = raw_class_df[raw_class_df.class_prop >= agg_users]

    # Aggregate information unique to clips and frames
    if subj_type == "clip":
        # Extract the median of the second where the animal/object is and number of animals
        raw_class_df = raw_class_df.groupby(["subject_ids", "label"], as_index=False)
        raw_class_df = pd.DataFrame(raw_class_df[["how_many", "first_seen"]].median())

    if subj_type == "frame":
        
        ############################ To update from here #######
        
        # Get prepared annotations
        raw_class_df = raw_class_df[raw_class_df["x"].notnull()]

        new_rows = []
        col_list = list(agg_annot_df.columns)

        x_pos, y_pos, w_pos, h_pos, user_pos, subject_id_pos = (
            col_list.index("x"),
            col_list.index("y"),
            col_list.index("w"),
            col_list.index("h"),
            col_list.index("user_name"),
            col_list.index("subject_ids"),
        )

        for name, group in agg_annot_df.groupby(["movie_id", "label", "frame_number"]):
            movie_id, label, start_frame = name

            total_users = agg_class_df[
                (agg_class_df.movie_id == movie_id)
                & (agg_class_df.label == label)
                & (agg_class_df.start_frame == start_frame)
            ]["user_name"].nunique()

            # Filter bboxes using IOU metric (essentially a consensus metric)
            # Keep only bboxes where mean overlap exceeds this threshold
            indices, new_group = filter_bboxes(
                total_users=total_users,
                users=[i[user_pos] for i in group.values],
                bboxes=[
                    np.array([i[x_pos], i[y_pos], i[w_pos], i[h_pos]])
                    for i in group.values
                ],
                obj=agg_obj,
                eps=agg_iou,
                iua=agg_iua,
            )

            subject_ids = [i[subject_id_pos] for i in group.values[indices]]

            for ix, box in zip(subject_ids, new_group):
                new_rows.append(
                    (
                        movie_id,
                        label,
                        start_frame,
                        ix,
                    )
                    + tuple(box)
                )

        agg_class_df = pd.DataFrame(
            new_rows,
            columns=[
                "movie_id",
                "label",
                "start_frame",
                "subject_id",
                "x",
                "y",
                "w",
                "h",
            ],
        )

    print("Classifications aggregated")
    return agg_class_df


def process_clips(df: pd.DataFrame, subjects: pd.DataFrame):

    # Create an empty list
    rows_list = []

    # Loop through each classification submitted by the users
    for index, row in df.iterrows():
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
                            elif "FIRSTTIME" not in k and "INDIVIDUAL" not in k:
                                f_time, inds = None, None

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
    annot_df = pd.DataFrame(
        rows_list, columns=["classification_id", "label", "first_seen", "how_many"]
    )

    # Specify the type of columns of the df
    annot_df["how_many"] = pd.to_numeric(annot_df["how_many"])
    annot_df["first_seen"] = pd.to_numeric(annot_df["first_seen"])

    # Add subject id to each annotation
    annot_df = pd.merge(
        annot_df,
        df.drop(columns=["annotations"]),
        how="left",
        on="classification_id",
    )
    
    # Add cleaned subject information based on subject_ids
    annot_df = pd.merge(
        annot_df,
        subjects,
        how="left",
        left_on="subject_ids",
        right_on="id",
    ) 

    #Select only relevant columns
    annot_df = annot_df[
        [
            "classification_id",
            "label",
            "how_many", 
            "first_seen",
            "user_name",
            "subject_ids",
            "movie_id",
        ]
    ]
    
    return pd.DataFrame(annot_df)


def process_frames(df: pd.DataFrame, subjects: pd.DataFrame):

    # Create an empty list
    rows_list = []
    
    # Loop through each classification submitted by the users and flatten them
    for index, row in df.iterrows():
        # Load annotations as json format
        annotations = json.loads(row["annotations"])

        # Select the information from all the labelled animals (e.g. task = T0)
        for ann_i in annotations:
            if ann_i["task"] == "T0":

                # Select each species annotated and flatten the relevant answers
                for i in ann_i["value"]:
                    choice_i = {
                        "classification_id": row["classification_id"],
                        # If value_i is not empty flatten labels
                        "x": int(i["x"]) if "x" in i else None,
                        "y": int(i["y"]) if "y" in i else None,
                        "w": int(i["width"]) if "width" in i else None,
                        "h": int(i["height"]) if "height" in i else None,
                        "label": str(i["tool_label"]) if "tool_label" in i else None,
                    }

                    rows_list.append(choice_i)

    # Create a data frame with annotations as rows
    flat_annot_df = pd.DataFrame(
        rows_list, columns=["classification_id", "x", "y", "w", "h", "label"]
    )
    
    # Add other classification information to the flatten classifications
    annot_df = pd.merge(
        flat_annot_df,
        df,
        how="left",
        on="classification_id",
    ) 

    # Add cleaned subject information based on subject_ids
    annot_df = pd.merge(
        annot_df,
        subjects,
        how="left",
        left_on="subject_ids",
        right_on="id",
    ) 
    
    #Select only relevant columns
    annot_df = annot_df[
        [
            "classification_id",
            "x", "y", "w", "h", 
            "label",
            "user_name",
            "subject_ids",
            "movie_id",
            "frame_number",
            "frame_exp_sp_id",
        ]
    ]
    
    return pd.DataFrame(annot_df)


def view_subject(subject_id: int, df: pd.DataFrame, annot_df: pd.DataFrame):
    try:

        subject_location = df[df.subject_ids == subject_id]["locations"].iloc[0]
    except:
        raise Exception("The reference data does not contain media for this subject.")
    if len(annot_df[annot_df.subject_ids == subject_id]) == 0:
        raise Exception("Subject not found in provided annotations")

    # Get the HTML code to show the selected subject
    if ".mp4" in subject_location:
        html_code = f"""
        <html>
        <div style="display: flex; justify-content: space-around">
        <div>
          <video width=500 controls>
          <source src={subject_location} type="video/mp4">
        </video>
        </div>
        <div>{annot_df[annot_df.subject_ids == subject_id]['label'].value_counts().sort_values(ascending=False).to_frame().to_html()}</div>
        </div>
        </html>"""
    else:
        html_code = f"""
        <html>
        <div style="display: flex; justify-content: space-around">
        <div>
          <img src={subject_location} type="image/jpeg" width=500>
        </img>
        </div>
        <div>{annot_df[annot_df.subject_ids == subject_id]['label'].value_counts().sort_values(ascending=False).to_frame().to_html()}</div>
        </div>
        </html>"""
    return HTML(html_code)


def launch_viewer(df: pd.DataFrame, total_df: pd.DataFrame):

    v = widgets.ToggleButtons(
        options=["Frames", "Clips"],
        description="Subject type:",
        disabled=False,
        button_style="success",
    )

    subject_df = df

    def on_tchange(change):
        global subject_df
        with main_out:
            if change["type"] == "change" and change["name"] == "value":
                clear_output()
                subject_df = df
                w = widgets.Combobox(
                    options=list(subject_df.subject_ids.apply(str).unique()),
                    description="Subject id:",
                    ensure_option=True,
                    disabled=False,
                )
                w.observe(on_change)
                display(w)
                global out
                out = widgets.Output()
                display(out)

    def on_change(change):
        global subject_df
        with out:
            if change["type"] == "change" and change["name"] == "value":
                a = view_subject(int(change["new"]), subject_df, total_df)
                clear_output()
                display(a)

    v.observe(on_tchange)
    display(v)
    main_out = widgets.Output()
    display(main_out)

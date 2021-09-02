import pandas as pd
import numpy as np
import json, io
from ast import literal_eval
from utils.zooniverse_utils import auth_session
#from db_setup.process_frames import filter_bboxes
from utils import db_utils
from collections import OrderedDict
from IPython.display import HTML, display, update_display, clear_output
import ipywidgets as widgets
from ipywidgets import interact
import asyncio

def choose_agg_parameters():
    agg_users = widgets.FloatSlider(
        value=0.8,
        min=0,
        max=1.0,
        step=0.1,
        description='Aggregation threshold:',
        disabled=False,
        continuous_update=False,
        orientation='horizontal',
        readout=True,
        readout_format='.1f',
        display='flex',
        flex_flow='column',
        align_items='stretch',
        style= {'description_width': 'initial'}
    )
    display(agg_users)
    min_users = widgets.IntSlider(
        value=3,
        min=1,
        max=20,
        step=1,
        description='Min numbers of users:',
        disabled=False,
        continuous_update=False,
        orientation='horizontal',
        readout=True,
        readout_format='d',
        display='flex',
        flex_flow='column',
        align_items='stretch',
        style= {'description_width': 'initial'}
    )
    display(min_users)
    return agg_users, min_users


def choose_workflows(workflows_df):
    
    layout = widgets.Layout(width='auto', height='40px') #set width and height
    
    # TODO display the workflow ids and versions
    w1 = widgets.Dropdown(
        options = workflows_df.display_name.unique().tolist(),
        value = workflows_df.display_name.unique().tolist()[0],
        description = 'Workflow id:',
        disabled = False,
    )
    
    w2 = widgets.Dropdown(
        options = list(map(float, workflows_df.version.unique().tolist())),
        value = float(workflows_df.version.unique().tolist()[0]),
        description = 'Minimum workflow version:',
        disabled = False,
        display='flex',
        flex_flow='column',
        align_items='stretch',
        style= {'description_width': 'initial'}
    )
    
    w3 = widgets.Dropdown(
        options = ["frame", "clip"],
        value = "clip",
        description = 'Subject type:',
        disabled = False,
    )
   
    #w1.observe(on_change)
    display(w1)
    #w2.observe(on_change)
    display(w2)
    #w3.observe(on_change)
    display(w3)
    
    w1_id = workflows_df[workflows_df.display_name==w1].workflow_id.unique()[0]
    
    return w1_id, w2, w3
    
          
def get_classifications(workflow_id: int, workflow_version: float, subj_type, class_df, subjects_df):

    # Filter classifications of interest
    class_df = class_df[
        (class_df.workflow_id == workflow_id)
        & (class_df.workflow_version >= workflow_version)
    ].reset_index(drop=True)
    
    
    # Add information about the subject type
    try:
        class_df['subject_type'] = class_df["subject_data"].apply(lambda x: [v["subject_type"] for k,v in json.loads(x).items()][0])

        # Ensure only classifications of one type of subject get analysed (frame or video)
        class_df = class_df[class_df.subject_type == subj_type]
    except:
        pass
    
    # Add information on the location of the subject
    total_df = pd.merge(class_df, subjects_df[["subject_id", "workflow_id", "locations"]], 
               left_on=['subject_ids', "workflow_id"], right_on=["subject_id", "workflow_id"])
                
    total_df["locations"] = total_df["locations"].apply(lambda x: literal_eval(x)["0"])
    
    print("Zooniverse classifications have been retrieved")
    
    return total_df, class_df


def aggregrate_classifications(df, subj_type, agg_users, min_users):
    
    print("Aggregrating the classifications")
    
    # Process the classifications of clips or frames
    if subj_type=="clip":
        agg_class_df = process_clips(df)
    
    if subj_type=="frame":
        agg_class_df = process_frames(df)
   
    
    # Calculate the number of users that classified each subject
    agg_class_df["n_users"] = agg_class_df.groupby("subject_ids")[
        "classification_id"
    ].transform("nunique")

    # Select frames with at least n different user classifications
    agg_class_df = agg_class_df[agg_class_df.n_users >= min_users]
    
    # Calculate the proportion of users that agreed on their annotations
    agg_class_df["class_n"] = agg_class_df.groupby(["subject_ids", "label"])[
        "classification_id"
    ].transform("count")
    agg_class_df["class_prop"] = agg_class_df.class_n / agg_class_df.n_users

    # Select annotations based on agreement threshold
    agg_class_df = agg_class_df[agg_class_df.class_prop >= agg_users]

    # Aggregate information unique to clips and frames
    if subj_type=="clip":
        # Extract the median of the second where the animal/object is and number of animals
        agg_class_df = agg_class_df.groupby(["subject_ids", "label"], as_index=False)
        agg_class_df = pd.DataFrame(agg_class_df[["how_many", "first_seen"]].median())
    
    if subj_type=="frame":
        # TODO
        print("WIP")

    print("Classifications aggregated")
    return agg_class_df


def process_clips(df: pd.DataFrame):
    
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
    
    annot_df['retired'] = annot_df["subject_data"].apply(lambda x: [v["retired"] for k,v in json.loads(x).items()][0])
    
    return annot_df


def process_frames(df: pd.DataFrame):
    
    df["annotation"] = df["annotations"].apply(
        lambda x: literal_eval(x)[0]["value"], 1
    )

    # Extract annotation metadata
    df["annotation"] = df[
        ["movie_id", "frame_number", "label", "annotation", "user_name", "subject_id"]
    ].apply(
        lambda x: [
            OrderedDict(
                list(x["movie_id"].items())
                + list(x["frame_number"].items())
                + list(x["label"].items())
                + list(x["annotation"][i].items())
                + list(x["user_name"].items())
                + list(x["subject_id"].items())
            )
            for i in range(len(x["annotation"]))
        ]
        if len(x["annotation"]) > 0
        else [
            OrderedDict(
                list(x["movie_id"].items())
                + list(x["frame_number"].items())
                + list(x["label"].items())
                + list(x["user_name"].items())
                + list(x["subject_id"].items())
            )
        ],
        1,
    )

    # Convert annotation to format which the tracker expects
    ds = [
        OrderedDict(
            {
                "user": i["user_name"],
                "movie_id": i["movie_id"],
                "label": i["label"],
                "start_frame": i["frame_number"],
                "x": int(i["x"]) if "x" in i else None,
                "y": int(i["y"]) if "y" in i else None,
                "w": int(i["width"]) if "width" in i else None,
                "h": int(i["height"]) if "height" in i else None,
                "subject_id": int(i["subject_id"]) if "subject_id" in i else None,
            }
        )
        for i in df.annotation.explode()
        if i is not None and i is not np.nan
    ]

    return df

def view_subject(subject_id: int, df: pd.DataFrame, annot_df: pd.DataFrame):
    try:
        
        subject_location = df[df.subject_ids == subject_id]["locations"].iloc[0]
    except:
        raise Exception("The reference data does not contain media for this subject.")
    if len(annot_df[annot_df.subject_ids == subject_id]) == 0: 
        raise Exception("Subject not found in provided annotations")
       
    print(df[df.subject_ids == subject_id].columns)
    # Get the HTML code to show the selected subject
    if ".mp4" in subject_location:
        html_code =f"""
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
        html_code =f"""
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
        options=['Frames', 'Clips'],
        description='Subject type:',
        disabled=False,
        button_style='success',
    )

    subject_df = df

    def on_tchange(change):
        global subject_df
        with main_out:
            if change['type'] == 'change' and change['name'] == 'value':
                clear_output()
                subject_df = df
                w = widgets.Combobox(
                    options=subject_df.subject_ids.unique().tolist(),
                    description='Subject id:',
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
            if change['type'] == 'change' and change['name'] == 'value':
                a = view_subject(change['new'], subject_df, total_df)
                clear_output()
                display(a)

    v.observe(on_tchange)
    display(v)
    main_out = widgets.Output()
    display(main_out)
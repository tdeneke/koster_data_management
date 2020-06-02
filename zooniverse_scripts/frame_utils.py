import os, sys, re
import argparse
import pims
import db_utils
import numpy as np
import cv2 as cv
import pandas as pd


def drawBoxes(df, movie_dir, out_path):
    df["movie_path"] = movie_dir + df["filename"].apply(
        lambda x: os.path.basename(x.rsplit("_frame_")[0]) + ".mov"
    )
    df["annotation"] = df[["x_position", "y_position", "width", "height"]].apply(
        lambda x: tuple([x[0], x[1], x[2], x[3]]), 1
    )
    df = df.drop(columns=["x_position", "y_position", "width", "height"])
    for name, group in df.groupby(
        ["movie_path", "frame_number", "species_id", "filename"]
    ):
        frame = pims.Video(name[0])[name[1]]
        boxes = [tuple(i[4:])[0] for i in group.values]
        for box in boxes:
            # Calculating end-point of bounding box based on starting point and w, h
            end_box = tuple([int(box[0] + box[1]), int(box[2] + box[3])])
            # changed color and width to make it visible
            cv.rectangle(frame, (int(box[0]), int(box[1])), end_box, (255, 0, 0), 1)
        if not os.path.exists(out_path):
            os.mkdir(out_path)
        cv.imwrite(out_path + os.path.basename(name[3]), frame)
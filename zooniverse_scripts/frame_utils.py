import os, sys, re
import argparse
import pims
import db_utils
import numpy as np
import cv2 as cv
import pandas as pd

# Draw the predicted bounding box
def drawBox(movie_dir, boxes, img_path, out_path):
	movie_file = movie_dir + os.path.basename(img_path.rsplit("_frame_")[0]) + ".mov"
	frame_number = int(re.findall(r"(?<=[_frame_])\d+(?=_)", img_path)[0])
	frame = pims.Video(movie_file)[frame_number]
    for i in range(0, len(boxes)):
    	# Calculating end-point of bounding box based on starting point and w, h
    	end_box = tuple(boxes[i][0] + boxes[i][2], boxes[i][1] + boxes[i][3]) 
        # changed color and width to make it visible
        cv.rectangle(frame, tuple(boxes[i][:2]), end_box, (255, 0, 0), 1)
    cv.imwrite(out_path+os.path.basename(img_path), frame)






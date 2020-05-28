import os, sys
import argparse
import numpy as np
import cv2 as cv
import pims

# Draw the predicted bounding box
def drawBox(movie_dir, boxes, img_path, out_path):
	movie_file = movie_dir + os.path.basename(path.rsplit("_frame_")[0]) + ".mov"
	frame_number = re.findall(r"(?<=[_frame_])\d+(?=_)", path)
	frame = pims.Video(movie_file)[frame_number]
    for i in range(0, len(boxes)):
        # changed color and width to make it visible
        cv2.rectangle(image, tuple(boxes[i][:2]), tuple(boxes[i][2:]), (255, 0, 0), 1)
    cv2.imwrite(out_path+os.path.basename(img_path), image)







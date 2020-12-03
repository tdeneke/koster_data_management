import os
import subprocess
import math
import csv
from datetime import date

#Define a function to find out the time to start trimming
def ceildiv(a, b):
    return int(math.ceil(a / float(b)))

#Specify the sparation time between clips
split_chunk = 300

#Create the directory to save all the new clips
if not os.path.exists("subjects"):
    os.makedirs("subjects")

#Create an empty csvfile "manuscript" to keep track of the clips generated
with open('subjects/manuscript.csv', 'w') as csvfile:
    filewriter = csv.writer(csvfile, delimiter=',',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
    filewriter.writerow(['filename', '#start_time', '#end_time'])


#Loop through each movie and split them into 10 seconds
for file in os.listdir("."):
    if file.endswith(".mov"):
        #Specify the output folders
        filename = file.split(".")[0]

        #Estimate the length of the video
        output = subprocess.check_output(("ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", file)).strip()
        video_length = int(float(output))

        #Calculate the initial time to start trimming (every 5 minutes ~300seconds)
        split_count = ceildiv(video_length, split_chunk)

        if(split_count == 1):
            print("Video length is less then the target split length.")
            raise SystemExit

        #Generate a manuscript
        for n in range(0, split_count):
            if n == 0:
                split_start = 0
            else:
                split_start = float(split_chunk) * n
            fileoutput = "subjects/" + str(filename) + "_" + str(int(split_start)) + ".mp4"
            subprocess.call(["ffmpeg", "-ss", str(split_start), "-t", "10", "-i", file, "-c", "copy", "-force_key_frames", "1", fileoutput])
            #Add a line to the manuscript to keep track of the clips generated
            file_output = fileoutput.split("/")[-1]
            row = [file_output, split_start, split_start+10]
            with open('subjects/manuscript.csv','a') as csvFile:
                writer = csv.writer(csvFile)
                writer.writerow(row)
            csvFile.close()

#Rename the csv file to include today's date
csv_date = "subjects/manuscript_" + date.today().strftime("%d_%m_%Y") + ".csv"
os.rename('subjects/manuscript.csv', csv_date)

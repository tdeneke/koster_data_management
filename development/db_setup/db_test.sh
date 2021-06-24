#!/bin/bash

# Remove the demo database
rm $3

# Set up the database
python -Wignore init.py --db_path $3
status=$?
if [ $status -ne 0 ]; then
  echo "Database setup failed: $status"
  exit $status
fi

# Populate static database tables
python -Wignore static.py --species_file_id 'https://drive.google.com/file/d/1dnueH3BjJrMK8buVjfyFbxfu0E-5dX7Z/view?usp=sharing' --movies_file_id 'https://drive.google.com/file/d/1xYcmMUjAawnYIyti9QNTs-oBf8XshJvs/view?usp=sharing' --db_path $3 --movies_path $4
status=$?
if [ $status -ne 0 ]; then
  echo "Static tables setup failed: $status"
  exit $status
fi

# Synchronise Zooniverse and Koster DB
python -Wignore subjects_uploaded.py -u $1 -p $2 -db $3
status=$?
if [ $status -ne 0 ]; then
  echo "Synchronisation failed: $status"
  exit $status
fi

# Aggregate clip classifications from Zooniverse
python -Wignore process_clips.py -u $1 -p $2 -db $3 -du $8
status=$?
if [ $status -ne 0 ]; then
  echo "Clip aggregation failed: $status"
  exit $status
fi

# Aggregate frame annotations from Zooniverse 
python -Wignore process_frames.py -u $1 -p $2 -db $3 -obj $5 -eps $6 -iua $7 -du $8 -nu $9
status=$?
if [ $status -ne 0 ]; then
  echo "Frame aggregation failed: $status"
  exit $status
fi


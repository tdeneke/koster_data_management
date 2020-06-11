#!/bin/bash

# Remove the demo database
rm $3
cd db_setup

# Set up the database
python -Wignore init.py --db_path $3
status=$?
if [ $status -ne 0 ]; then
  echo "Database setup failed: $status"
  exit $status
fi

# Populate static database tables
python -Wignore static.py --species_file_id '1dnueH3BjJrMK8buVjfyFbxfu0E-5dX7Z' --movies_file_id '1LL-Ah_FIkBiGKEldYvuhNeL2NyOvKBip' --db_path $3 --movies_path $4
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
python -Wignore process_clips.py -u $1 -p $2 -db $3
status=$?
if [ $status -ne 0 ]; then
  echo "Clip aggregation failed: $status"
  exit $status
fi

# Aggregate frame annotations from Zooniverse 
python -Wignore process_frames.py -u $1 -p $2 -db $3 -obj $5 -eps $6 -iua $7
status=$?
if [ $status -ne 0 ]; then
  echo "Frame aggregation failed: $status"
  exit $status
fi


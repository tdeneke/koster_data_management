#!/bin/bash

# Remove the demo database
rm $3

# Set up the database
python db_setup.py --db_path $3
status=$?
if [ $status -ne 0 ]; then
  echo "Database setup failed: $status"
  exit $status
fi

# Populate static database tables
python db_static.py --species_file_id '1dnueH3BjJrMK8buVjfyFbxfu0E-5dX7Z' --movies_file_id '1LL-Ah_FIkBiGKEldYvuhNeL2NyOvKBip' --db_path $3 --movies_path $4
status=$?
if [ $status -ne 0 ]; then
  echo "Static tables setup failed: $status"
  exit $status
fi

# Synchronise Zooniverse and Koster DB
python db_uploaded.py -u $1 -p $2 -db $3
status=$?
if [ $status -ne 0 ]; then
  echo "Synchronisation failed: $status"
  exit $status
fi

# Aggregate clip classifications from Zooniverse
python process_clips.py -u $1 -p $2 -db $3
status=$?
if [ $status -ne 0 ]; then
  echo "Clip aggregation failed: $status"
  exit $status
fi

# Aggregate frame annotations from Zooniverse 
python process_frames.py -u $1 -p $2 -db $3
status=$?
if [ $status -ne 0 ]; then
  echo "Frame aggregation failed: $status"
  exit $status
fi


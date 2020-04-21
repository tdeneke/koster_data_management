#!/bin/bash

rm $3 &&
python db_setup.py --db_path $3 &&
python db_static.py --species_file_id '1dnueH3BjJrMK8buVjfyFbxfu0E-5dX7Z' --movies_file_id '1LL-Ah_FIkBiGKEldYvuhNeL2NyOvKBip' --db_path $3 &&
python db_manual_upload.py -u $1 -p $2 -db $3 &&
python process_clips.py -u $1 -p $2 -db $3



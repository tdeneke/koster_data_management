sql = """CREATE TABLE workflows
(
workflow_id varchar(255) PRIMARY KEY,
display_name text,
version integer,
retired_set_member_subjects_count integer NULL
);

CREATE TABLE subjects
(
subject_id varchar(255) PRIMARY KEY,
workflow_id varchar(255) NULL,
subject_set_id varchar(255),
classifications_count integer NULL,
retired_at datetime NULL,
retirement_criteria text NULL,
zoo_upload_date datetime,
clip_filename integer,
FOREIGN KEY (workflow_id) REFERENCES workflows (workflow_id)
);

CREATE TABLE user_info
(
user_id varchar(255) PRIMARY KEY,
user_name text
);

CREATE TABLE classifications
(
classification_id int PRIMARY KEY,
user_id varchar(255),
workflow_id varchar(255),
created_at datetime,
started_at datetime,
finished_at datetime,
subject_id varchar(255),
FOREIGN KEY (user_id) REFERENCES user_info (user_id)
);

CREATE TABLE annotations
(
task_id integer PRIMARY KEY,
task_label varchar(255),
classification_id varchar(255),
created_at datetime,
started_at datetime,
finished_at datetime,
subject_id varchar(255),
FOREIGN KEY (classification_id) REFERENCES classifications (classification_id),
FOREIGN KEY (subject_id) REFERENCES subjects (subject_id)
);

CREATE TABLE agg_classifications
(
subject_id varchar(255) PRIMARY KEY,
classifications int,
choice varchar(255),
choice_v_f int NULL,
how_many int NULL,
how_many_vf int NULL,
Resting int NULL,
Standing int NULL,
Moving int NULL,
Eating int NULL,
Interacting int NULL,
Young int NULL,
No_Young int NULL,
Horns int NULL,
No_Horns int NULL,
Dont_care_yes int NULL,
Dont_care_no int NULL,
FOREIGN KEY (subject_id) REFERENCES subjects (subject_id)
);

/*CREATE TABLE agg_annotations
(
subject_ids varchar(255) PRIMARY KEY,
labels varchar(255),
positions varchar(255),
);

CREATE TABLE value_wf1
(
choice_id integer PRIMARY KEY,
choice varchar(255),
individuals integer,
first_seen integer,
task_id integer,
FOREIGN KEY (task_id) REFERENCES tasks (task_id)
);*/

CREATE TABLE movies
(
filename text PRIMARY KEY,
movie_date datetime NULL,
duration datetime NULL,
author text NULL,
location text NULL,
organisation text NULL
); 

CREATE TABLE clips
(
filename text PRIMARY KEY,
panoptes_filename text NULL,
start_time datetime,
end_time datetime,
clip_date datetime NULL,
movie_filename text,
FOREIGN KEY (movie_filename) REFERENCES movies (filename)
);



"""

# CREATE TABLE IF NOT EXISTS value_wf2
# (
#  integer PRIMARY KEY,
# task_label varchar(255),
# classification_id varchar(255),
# created_at datetime,
# started_at datetime,
# finished_at datetime,
# subject_id varchar(255),
# FOREIGN KEY (classification_id) REFERENCES classifications (classification_id)
# FOREIGN KEY (subject_id) REFERENCES subjects (subject_id)
# );

# CREATE TABLE IF NOT EXISTS sites
# (
# classification_id int,
# user_id varchar(255),
# workflow_id varchar(255),
# created_at datetime,
# started_at datetime,
# finished_at datetime,
# subject_id varchar(255),
# );

# CREATE TABLE IF NOT EXISTS frames
# (
# classification_id int,
# user_id varchar(255),
# workflow_id varchar(255),
# created_at datetime,
# started_at datetime,
# finished_at datetime,
# subject_id varchar(255),
# );

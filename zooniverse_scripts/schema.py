sql = """CREATE TABLE IF NOT EXISTS sites
(
id integer PRIMARY KEY AUTOINCREMENT,
name text NULL,
coord_lat varchar(255) NULL,
coord_lon varchar(255) NULL,
protected varchar(255) NULL
);

CREATE TABLE IF NOT EXISTS movies
(
id integer PRIMARY KEY AUTOINCREMENT,
filename text NULL,
created_on datetime NULL,
duration datetime NULL,
author text NULL,
location text NULL,
organisation text NULL,
path text NULL,
site_id integer,
FOREIGN KEY (site_id) REFERENCES sites (id)
); 

CREATE TABLE IF NOT EXISTS clips
(
id integer PRIMARY KEY,
filename text NULL,
start_time datetime,
end_time datetime,
clipped_date datetime NULL,
movie_id integer,
FOREIGN KEY (movie_id) REFERENCES movies (id)
);

CREATE TABLE IF NOT EXISTS subjects
(
id integer PRIMARY KEY,
workflow_id varchar(255) NULL,
subject_set_id varchar(255),
classifications_count integer NULL,
retired_at datetime NULL,
retirement_criteria text NULL,
zoo_upload_date datetime,
clip_id integer,
FOREIGN KEY (clip_id) REFERENCES clips (id)
);

CREATE TABLE IF NOT EXISTS species
(
id integer PRIMARY KEY AUTOINCREMENT,
label text
);

CREATE TABLE IF NOT EXISTS agg_annotations_clip
(
id integer PRIMARY KEY,
species_id varchar(255),
how_many integer,
first_seen integer,
clip_id integer,
FOREIGN KEY (clip_id) REFERENCES clips (id),
FOREIGN KEY (species_id) REFERENCES species (id)
);

CREATE TABLE IF NOT EXISTS agg_annotations_frame
(
id integer PRIMARY KEY,
species_id varchar(255),
x_position integer,
frame_number integer,
clip_id integer,
FOREIGN KEY (clip_id) REFERENCES clips (id)
);
"""

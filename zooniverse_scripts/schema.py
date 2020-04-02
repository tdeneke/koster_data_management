sql = """CREATE TABLE movies
(
id integer PRIMARY KEY,
filename text NULL,
created_on datetime NULL,
duration datetime NULL,
author text NULL,
location text NULL,
organisation text NULL
); 

CREATE TABLE clips
(
id integer PRIMARY KEY,
filename text NULL,
start_time datetime,
end_time datetime,
clipped_date datetime NULL,
movie_id integer,
FOREIGN KEY (movie_id) REFERENCES movies (id)
);

CREATE TABLE frames
(
id integer PRIMARY KEY,
frame_number integer,
movie_time datetime,
movie_id integer,
FOREIGN KEY (movie_id) REFERENCES movies (id)
);

CREATE TABLE subjects
(
id integer PRIMARY KEY,
workflow_id varchar(255) NULL,
subject_set_id varchar(255),
classifications_count integer NULL,
retired_at datetime NULL,
retirement_criteria text NULL,
zoo_upload_date datetime,
clip_id integer,
frame_id integer,
FOREIGN KEY (clip_id) REFERENCES clips (id),
FOREIGN KEY (frame_id) REFERENCES frames (id)
);

CREATE TABLE species
(
id varchar PRIMARY KEY,
label text
);

CREATE TABLE agg_annotations_clip
(
id integer PRIMARY KEY,
species_id varchar(255),
how_many integer,
first_seen integer,
clip_id integer,
FOREIGN KEY (clip_id) REFERENCES clips (id),
FOREIGN KEY (species_id) REFERENCES species (id)
);

CREATE TABLE agg_annotations_frame
(
id integer PRIMARY KEY,
species_id varchar(255),
x_position integer,
frame_id integer,
FOREIGN KEY (frame_id) REFERENCES frames (id)
);


"""

# CREATE TABLE IF NOT EXISTS sites
# (
# id integer PRIMARY KEY,
# name text,
# coord varchar(255),
# protected varchar(255)
# );


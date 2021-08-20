==================
Koster Seafloor Observatory - Data management
==================

The Koster Seafloor Observatory is an open-source, citizen science and machine learning approach to analyse subsea movies. Find out more about it at https://www.zooniverse.org/projects/victorav/the-koster-seafloor-observatory

.. image:: https://panoptes-uploads.zooniverse.org/project_attached_image/c4912e31-b7c2-4076-a5d4-4546fca5e231.png
    :alt: "(Overview of the three main modules and the components of the Koster Seafloor Observatory.")
    

Overview
------------

This repository contains scripts related to the Data Management component of KSO, specifically the hot server. 

KSO creates a SQLite database to link all information related to the movies and the classifications provided by both citizen scientists and machine learning algorithms. The database has seven interconnected tables (Fig. 2). The “movies”, “sites” and “species” tables have project-specific information from the underwater movie metadata, as well as the species choices available for citizen scientists to annotate the clips, retrieved from Zooniverse. The “agg_annotations_frame” and “agg_annotations_clip” tables contain information related to the annotations provided by citizen scientists. The “subjects” table has information related to the clips and frames uploaded to the Koster Seafloor Observatory. The "model_annotations" table holds information related to the annotations provided by the machine learning algorithms. The database follows the Darwin Core (DwC) standards to maximise the sharing, use and reuse of open-access biodiversity data.

.. image:: https://panoptes-uploads.zooniverse.org/project_attached_image/61225451-fb50-4b35-8ef4-91a065e7ff50.png
    :alt: "(Entity relationship diagram of the SQLite database of the Koster Seafloor Observatory.")

Currently, the system is built around a series of tutorials (python notebooks) that users can run to: 

* Upload videos to the server
* Upload videos to a Zooniverse project
* Upload frames to a Zooniverse project
* Analyse Zooniverse classifications
* Download and format Zooniverse classifications 

Requirements
------------

* Python 3.7+
* Python dependencies listed in requirements.txt

Installation Instructions
-------------------------

CLone this repository
~~~~

Follow the `instructions to clone a Github repo
<https://docs.github.com/en/github/creating-cloning-and-archiving-repositories/cloning-a-repository-from-github/cloning-a-repository/>`_ 


Create the inital information for the database 
~~~~

The “movies”, “sites” and “species”. You can `download a template of the csv files<https://drive.google.com/drive/folders/1_3ooMI1wgGnsv7dby8V1OIR9UWekcGJV?usp=sharing/>`.



Documentation
~~~~~~~~~~~~~



Citation
--------

If you use this code or its models in your research, please cite:

Anton V, Germishuys J, Bergström P, Lindegarth M, Obst M (2021) An open-source, citizen science and machine learning approach to analyse subsea movies. Biodiversity Data Journal 9: e60548. https://doi.org/10.3897/BDJ.9.e60548

Collaborations/questions
~~~~~~~~~~~~

We are working to make our work to other marine scientists. Please feel free to `contact us`_ with your questions.

.. _contact us: matthias.obst@marine.gu.se

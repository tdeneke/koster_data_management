==================
Koster Seafloor Observatory - Data management
==================

The Koster Seafloor Observatory is an open-source, citizen science and machine learning approach to analyse subsea movies.

.. image:: images/high-level-overview.png
   :align: center
   :alt: "(Overview of the three main modules and the components of the Koster Seafloor Observatory.")
    
You can find out more about the project at https://www.zooniverse.org/projects/victorav/the-koster-seafloor-observatory

Overview
------------

This repository contains scripts related to the Data Management component of KSO, specifically the hot server. 

KSO creates a SQLite database to link all information related to the movies and the classifications provided by both citizen scientists and machine learning algorithms. The database has seven interconnected tables. The “movies”, “sites” and “species” tables have project-specific information from the underwater movie metadata, as well as the species choices available for citizen scientists to annotate the clips, retrieved from Zooniverse. The “agg_annotations_frame” and “agg_annotations_clip” tables contain information related to the annotations provided by citizen scientists. The “subjects” table has information related to the clips and frames uploaded to the Koster Seafloor Observatory. The "model_annotations" table holds information related to the annotations provided by the machine learning algorithms. The database follows the `Darwin Core (DwC) <https://dwc.tdwg.org/simple/>`_  standards to maximise the sharing, use and reuse of open-access biodiversity data.



.. image:: https://panoptes-uploads.zooniverse.org/project_attached_image/61225451-fb50-4b35-8ef4-91a065e7ff50.png
   :align: center
   :alt: "(Entity relationship diagram of the SQLite database of the Koster Seafloor Observatory.")

Currently, the system is built around a series of tutorials (Jupyter notebooks) that users can run to: 

* Upload videos to the server
* Upload videos to a Zooniverse project
* Upload frames to a Zooniverse project
* Analyse Zooniverse classifications
* Download and format Zooniverse classifications 

Quickstart
--------------------

.. image:: https://mybinder.org/badge_logo.svg
   :target: https://mybinder.org/v2/gh/ocean-data-factory-sweden/koster_data_management/main


Requirements
------------

* Python 3.7+
* Python dependencies listed in requirements.txt

Installation Instructions
-------------------------

Clone this repository
~~~~

For those with a Github account or git installed simply clone this
repository using
``git clone https://github.com/ocean-data-factory-sweden/koster_data_management.git``
followed by ``cd koster_data_management``.

What if I don't have Github installed?

-  Option 1: Github installation

   -  Windows: Download and install `here`_
   -  Mac (Homebrew): ``brew install git``
   -  Linux (Debian & Ubuntu): ``sudo apt-get install git``

-  Option 2: Manual download

   -  Find “Clone or Download” button on repository website and click
      “Download zip”

Comprehensive `instructions to clone a Github repo
<https://docs.github.com/en/github/creating-cloning-and-archiving-repositories/cloning-a-repository-from-github/cloning-a-repository/>`_ 

Download `Anaconda <https://docs.anaconda.com/anaconda/install/index.html>`. Anaconda allows you to create virtual Python environments for and features a simple package manager to keep track of dependencies. 

Create the initial information for the database 
~~~~

You will need to create a directory in the db_starter folder with information about the movies, sites and species of interest. You can `download a template of the csv files <https://drive.google.com/file/d/1PZGRoSY_UpyLfMhRphMUMwDXw4yx1_Fn/view?usp=sharing/>`_ or specify the Google Drive ID of a zipped folder containing these files (e.g. 1PZGRoSY_UpyLfMhRphMUMwDXw4yx1_Fn).


Link your movies to the database 
~~~~

You will need underwater movies to run KSO. You can `download some samples <https://drive.google.com/drive/folders/1t2ce8euh3SEU2I8uhiZN1Tu-76ZDqB6w?usp=sharing/>`_. Remember where you store the movies as you will need to specify the directory of the movies in the tutorials.



Citation
--------

If you use this code or its models in your research, please cite:

Anton V, Germishuys J, Bergström P, Lindegarth M, Obst M (2021) An open-source, citizen science and machine learning approach to analyse subsea movies. Biodiversity Data Journal 9: e60548. https://doi.org/10.3897/BDJ.9.e60548

Collaborations/questions
~~~~~~~~~~~~

We are working to make our work to other marine scientists. Please feel free to `contact us`_ with your questions.

.. _contact us: matthias.obst@marine.gu.se

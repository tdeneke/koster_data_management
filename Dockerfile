# Koster Object Detection - Koster Lab Database
# author: Jannes Germishuys

FROM python:3.6-slim

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git && \
    apt-get install -y vim && \
    apt-get install -y libglib2.0-0 && \
    apt-get install -y libsm6 libxext6 libxrender-dev && \
    apt-get install -y ffmpeg

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

ADD https://api.github.com/repos/ocean-data-factory-sweden/koster_lab_development/git/refs/heads/master version.json
RUN git clone -b master https://github.com/ocean-data-factory-sweden/koster_lab_development.git
WORKDIR /usr/src/app/koster_lab_development
RUN pip3 install -r requirements.txt

WORKDIR /usr/src/app/koster_lab_development/zooniverse_scripts


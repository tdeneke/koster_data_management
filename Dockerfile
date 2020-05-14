# Koster Object Detection - Koster Lab Database
# author: Jannes Germishuys

FROM python:3.6-slim

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git && \
    apt-get install -y libglib2.0-0 && \
    apt-get install -y libsm6 libxext6 libxrender-dev

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
RUN git clone https://github.com/ocean-data-factory-sweden/koster_lab_development
WORKDIR /usr/src/app/koster_lab_development/zooniverse_scripts

RUN pip3 install -r requirements.txt


FROM python:3.8-slim-buster

COPY . /base
WORKDIR /base
RUN pip install --break-system-packages -r requirements.txt

RUN apt-get update
RUN apt-get -y install sudo
RUN apt-get update
RUN apt-get install -y docker.io
RUN apt-get update 

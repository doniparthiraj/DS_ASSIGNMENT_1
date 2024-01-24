FROM python:3.8-slim-buster

COPY . /LoadBalancer
WORKDIR /LoadBalancer
EXPOSE 5000

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
RUN pip install requests
RUN apt-get update
RUN apt-get -y install sudo
RUN apt-get update && apt-get install -y docker.io

RUN apt-get update
CMD ["python3","LB.py"]
FROM mysql:8.0-debian
COPY deploy.sh /always-initdb.d/
COPY . /Server
WORKDIR /Server

RUN apt-get update && apt-get install -y python3 python3-pip python3-venv

# Create a virtual environment
RUN python3 -m venv /venv

# Activate the virtual environment
ENV PATH="/venv/bin:$PATH"


EXPOSE 5000

COPY requirements.txt requirements.txt
RUN /venv/bin/pip install -r requirements.txt
RUN /venv/bin/pip install requests
RUN apt-get update
RUN apt-get -y install sudo
RUN apt-get update && apt-get install -y docker.io

RUN apt-get update
ENV MYSQL_ROOT_PASSWORD="abc" 
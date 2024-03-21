FROM mysql:8.0-debian

COPY . /base
WORKDIR /base

RUN apt-get update
RUN apt-get install -y \
    python3-full \
    python3-pip
RUN pip install --break-system-packages --upgrade pip


RUN pip install --break-system-packages -r requirements.txt

RUN apt-get update
RUN apt-get -y install sudo

RUN apt-get -y install apt-transport-https ca-certificates curl gnupg2 software-properties-common
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add -
RUN add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable"
RUN apt-get update
RUN apt-get -y install docker-ce-cli
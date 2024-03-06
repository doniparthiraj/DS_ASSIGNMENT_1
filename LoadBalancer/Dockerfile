FROM mysql:8.0-debian

COPY deploy.sh /always-initdb.d/

COPY . /LoadBalancer
WORKDIR /LoadBalancer

RUN apt-get update
RUN apt-get install -y python3
RUN apt-get install -y python3-pip

RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN chmod +x custom-entry.sh

RUN pip install requests
RUN apt-get update
RUN apt-get -y install sudo
RUN apt-get update && apt-get install -y docker.io

# ENV USER=theuser
# RUN adduser --home /home/$USER --disabled-password --gecos GECOS $USER \
#   && echo "$USER ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/$USER \
#   && chmod 0440 /etc/sudoers.d/$USER \
#   && groupadd docker \
#   && usermod -aG docker $USER \
#   && chsh -s /bin/zsh $USER
# USER $USER

ENV HOME=/home/$USER
ENV MYSQL_ROOT_PASSWORD="abc"

EXPOSE 5000

ENTRYPOINT ["/LoadBalancer/custom-entry.sh"]
CMD ["mysqld"]
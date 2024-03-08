FROM mysql:8.0-debian

COPY deploy.sh /always-initdb.d/
COPY . /ser
WORKDIR /ser

RUN apt-get update
RUN apt-get install -y \
    python3-full \
    python3-pip
RUN pip install --break-system-packages --upgrade pip

RUN pip install --break-system-packages -r requirements.txt
RUN chmod +x /ser/custom-entry.sh

ENV USER=theuser
RUN adduser --home /home/$USER --disabled-password --gecos GECOS $USER \
  && chsh -s /bin/zsh $USER
RUN touch /ser/topicLog.db
RUN chown $USER /ser/topicLog.db

USER $USER
ENV HOME=/home/$USER
ENV MYSQL_ROOT_PASSWORD="abc"

EXPOSE 5000

ENTRYPOINT ["/ser/custom-entry.sh"]
CMD ["mysqld"]
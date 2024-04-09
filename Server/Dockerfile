FROM baseimg

COPY deploy.sh /always-initdb.d/
COPY . /ser
WORKDIR /ser


RUN chmod +x /ser/custom-entry.sh

ENV USER=theuser
RUN adduser --home /home/$USER --disabled-password --gecos GECOS $USER \
  && chsh -s /bin/zsh $USER
RUN touch /ser/topicLog.db
RUN chown $USER /ser/topicLog.db

RUN chown -R $USER:$USER /ser
RUN chmod 755 /ser
USER $USER
ENV HOME=/home/$USER
ENV MYSQL_ROOT_PASSWORD="abc"

EXPOSE 5000

ENTRYPOINT ["/ser/custom-entry.sh"]
CMD ["mysqld"]
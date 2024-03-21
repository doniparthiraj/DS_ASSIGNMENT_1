FROM baseimg

COPY deploy.sh /always-initdb.d/

COPY . /lb
WORKDIR /lb

RUN chmod +x /lb/custom-entry.sh

ENV USER=theuser
RUN adduser --home /home/$USER --disabled-password --gecos GECOS $USER \
  && echo "$USER ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/$USER \
  && chmod 0440 /etc/sudoers.d/$USER \
  && groupadd docker \
  && usermod -aG docker $USER \
  && chsh -s /bin/zsh $USER
USER $USER

ENV HOME=/home/$USER
ENV MYSQL_ROOT_PASSWORD="abc"

EXPOSE 5000

ENTRYPOINT ["/lb/custom-entry.sh"]

CMD ["mysqld"]


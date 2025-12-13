FROM jepsen-mariadb-galera-db:latest

# Bootstrap Galera cluster
USER root
COPY bootstrap /tmp/galera-bootstrap
RUN chmod +x /tmp/galera-bootstrap
RUN chown mysql:mysql /tmp/galera-bootstrap
USER mysql
RUN /tmp/galera-bootstrap

ENTRYPOINT ["/usr/sbin/mariadbd"]

FROM debian:bookworm-slim

# Install packages
ENV DEBIAN_FRONTEND noninteractive
RUN apt update
RUN apt -y install wget curl
RUN wget https://r.mariadb.com/downloads/mariadb_repo_setup
RUN chmod +x mariadb_repo_setup
RUN ./mariadb_repo_setup
RUN apt upgrade -y
RUN apt install -y mariadb-server galera-4

# For some reason apt doesn't create the /run/mysql directory... but only here,
# not in my LXC Debian Bookworm containers? Trying to get this working has been
# *exhausting* :-[
RUN mkdir /run/mysqld
RUN chown mysql:mysql /run/mysqld

# Config files
COPY my.cnf /etc/mysql/mariadb.conf.d/90-jepsen.cnf
COPY galera.cnf /etc/mysql/mariadb.conf.d/99-jepsen-galera.cnf

USER mysql
ENTRYPOINT ["/usr/sbin/mariadbd"]

# PostgreSQL container for test databases from FMI master databases
# Adapted from official PostgreSQL Dockerfile:
# https://github.com/docker-library/postgres/
#
# https://hub.docker.com/_/centos/

FROM centos:7
MAINTAINER "fmi"

# Ability to override PGDATA from command line or docker-compose.yml
ENV PGDATA=/var/lib/pgsql/data/10

# Explicitly set user/group IDs
RUN groupadd -r postgres && useradd -r -g postgres postgres && \
   # Inject excludes into YUM config files.
   # https://wiki.postgresql.org/wiki/YUM_Installation
   sed -i '/\[base\]/,/gpgkey=/{/gpgkey=/s/$/\'$'\n''exclude = postgres*/;}' /etc/yum.repos.d/CentOS-Base.repo && \
   sed -i '/\[updates\]/,/gpgkey=/{/gpgkey=/s/$/\'$'\n''exclude = postgres*/;}' /etc/yum.repos.d/CentOS-Base.repo && \

   # Install newest PostgreSQL major version repo.
   # http://yum.postgresql.org/repopackages.php
   # Note: EPEL is required by postgis and also supplies pv.

   echo "ip_resolve=IPv4" >> /etc/yum.conf && \
   curl -4 "https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm" -o /tmp/epel-release-latest-7.noarch.rpm && \
   yum --nogpgcheck -y install /tmp/epel-release-latest-7.noarch.rpm && \

   yum -y update  && \
   yum -y localinstall https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-7-x86_64/pgdg-centos10-10-2.noarch.rpm && \
   yum -y install postgresql10 postgresql10-server  postgresql10-contrib postgresql10-libs postgis24_10 && \
   yum clean all && rm -rf /tmp/* && \
   yum -y install lsof

USER postgres

# The first command (only needed once) is to initialize the database in PGDATA.
# -E UTF-8 is needed here because the observation_cache schema is also UTF-8.
RUN /usr/pgsql-10/bin/initdb -E 'UTF-8' -D ${PGDATA}

COPY etc/postgresql.conf ${PGDATA}
COPY etc/pg_hba.conf ${PGDATA}

# Start the server and create database users.
# -w is needed because otherwise createuser would run before the database is up.
# The server is stopped gracefully in the last step to avoid an error in the next
# layer, where the server is required to be running again.
RUN /usr/pgsql-10/bin/pg_ctl -w -s -D ${PGDATA} -o "-p 5432" start && \
    createdb -E 'UTF-8' observation_cache && \
    /usr/pgsql-10/bin/psql -c "CREATE USER admin LOGIN SUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD 'admin_pw';" && \
    /usr/pgsql-10/bin/psql -c "CREATE USER cache_user WITH PASSWORD 'cache_pw';" && \
    /usr/pgsql-10/bin/psql observation_cache admin -c "CREATE SCHEMA cache;" && \
    /usr/pgsql-10/bin/psql observation_cache admin -c "CREATE EXTENSION postgis;" && \
    /usr/pgsql-10/bin/psql observation_cache admin -c "CREATE EXTENSION postgis_topology;" && \
    /usr/pgsql-10/bin/pg_ctl -w -s -D ${PGDATA} -o "-p 5432" stop && \
    /usr/pgsql-10/bin/pg_ctl -w -s -D ${PGDATA} -o "-p 5432" start && \
    /usr/pgsql-10/bin/psql observation_cache admin -c "GRANT CREATE ON DATABASE observation_cache TO cache_user;" && \
    /usr/pgsql-10/bin/psql observation_cache admin -c "GRANT DELETE ON ALL TABLES IN SCHEMA cache TO cache_user;" && \
    /usr/pgsql-10/bin/psql observation_cache admin -c "GRANT TRUNCATE ON ALL TABLES IN SCHEMA cache TO cache_user;" && \
    /usr/pgsql-10/bin/psql observation_cache admin -c "GRANT INSERT ON ALL TABLES IN SCHEMA cache TO cache_user;" && \
    /usr/pgsql-10/bin/psql observation_cache admin -c "GRANT UPDATE ON ALL TABLES IN SCHEMA cache TO cache_user;" && \
    /usr/pgsql-10/bin/psql observation_cache admin -c "GRANT SELECT ON ALL TABLES IN SCHEMA cache TO cache_user;" && \
    /usr/pgsql-10/bin/pg_ctl -w -s -D ${PGDATA} -o "-p 5432" stop && \
    chmod 0700 ${PGDATA}

# PostgreSQL is set to listen on TCP port 5445.
EXPOSE 5445

#Launch the daemon
ENTRYPOINT exec /usr/pgsql-10/bin/postgres -o "-p 5445" -D /var/lib/pgsql/data/10

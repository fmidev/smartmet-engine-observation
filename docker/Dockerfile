FROM postgres:10
LABEL maintainer "Anssi Reponen <anssi.reponen@fmi.fi>"

ENV POSTGIS_MAJOR 2.4
RUN apt-get update && \
     apt-get install -y postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR \
      postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR-scripts && \
                       rm -rf /var/lib/apt/lists/*

RUN mkdir -p /docker-entrypoint-initdb.d
COPY initdb-postgis.sh /docker-entrypoint-initdb.d/

# PostgreSQL is set to listen on TCP port 5432.
EXPOSE 5432

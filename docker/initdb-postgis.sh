psql -v ON_ERROR_STOP=1 "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
     CREATE USER cache_user WITH PASSWORD 'cache_pw';
     CREATE DATABASE observation_cache;
     GRANT ALL PRIVILEGES ON DATABASE observation_cache TO cache_user;
EOSQL
psql -v ON_ERROR_STOP=1 "$POSTGRES_USER" --dbname "observation_cache" <<-EOSQL
     CREATE EXTENSION IF NOT EXISTS postgis;
     CREATE EXTENSION IF NOT EXISTS postgis_topology;
EOSQL

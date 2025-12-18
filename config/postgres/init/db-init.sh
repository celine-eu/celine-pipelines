#!/bin/bash
set -eu

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" > /dev/null <<-EOSQL

  CREATE EXTENSION IF NOT EXISTS postgis CASCADE;
  CREATE EXTENSION IF NOT EXISTS postgis_topology CASCADE;

  -- Create Meltano database
  CREATE DATABASE meltano;

  -- Create Prefect database
  CREATE DATABASE prefect;

  -- Create Marquez database
  CREATE USER ${MARQUEZ_USER};
  ALTER USER ${MARQUEZ_USER} WITH PASSWORD '${MARQUEZ_PASSWORD}';

  CREATE DATABASE ${MARQUEZ_DB};
  GRANT ALL PRIVILEGES ON DATABASE ${MARQUEZ_DB} TO ${MARQUEZ_USER};

  \connect ${MARQUEZ_DB}
  GRANT ALL ON SCHEMA public TO ${MARQUEZ_USER};
  ALTER DATABASE ${MARQUEZ_DB} SET search_path TO public;

EOSQL
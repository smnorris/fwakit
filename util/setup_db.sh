#!/usr/bin/env bash
set -eu

# This script:
#  - downloads BC FWA data in .gdb format
#    (minus linear boundaries, watershed boundaries and watershed polys)
#  - loads select layers into a PostGIS-enabled PostgreSQL database
#  - adds ltree extention for fast upstream/downstream queries
#  - adds additional utility sql functions for working with FWA/BC data

# Database connection settings - edit as needed:
dbname=fishpassage
dbuser=postgres
dbhost=localhost
dbport=5432

psql="psql -q -h $dbhost -p $dbport -U $dbuser"

tmp="${TEMP:-/tmp}"

echo "Setting up PostGIS database..."
$psql -c "drop database $dbname;" || true  # useful to uncomment during dev
$psql -c "create database $dbname"
$psql -d $dbname -c "create extension postgis;"
$psql -d $dbname -c "create extension ltree;"
# to keep things simple all data goes to bcgw standard schema
$psql -d $dbname -c "create schema whse_basemapping;"
$psql -d $dbname -f "$(dirname $0)/../fwakit/sql/functions.sql"

echo "Downloading FWA data (3.5G)..."
wget --trust-server-names -qNP "$tmp" ftp://ftp.geobc.gov.bc.ca/sections/outgoing/bmgs/FWA_Public/FWA_BC.gdb.zip
unzip -qun -d "$tmp" "$tmp/FWA_BC.gdb.zip"

echo "Loading FWA data to PostGIS, fish habitat modelling required tables only"
echo "(add to list if other features are required)"
tables=('FWA_WATERSHED_GROUPS_POLY'
          'FWA_OBSTRUCTIONS_SP'
          'FWA_EDGE_TYPE_CODES'
          'FWA_STREAMS_20K_50K'
          'FWA_WATERBODIES_20K_50K'
          'FWA_WATERBODY_TYPE_CODES'
          'FWA_STREAM_NETWORKS_SP'
      )
# waterbodies are likely of interest for loading as well, but not required for
# fish passage modelling
# FWA_MANMADE_WATERBODIES_POLY
# FWA_RIVERS_POLY
# FWA_LAKES_POLY
# FWA_WETLANDS_POLY

for table in ${tables[@]}; do
    nln="$(echo $table | tr '[A-Z]' '[a-z]')"
    ogr2ogr \
      -progress \
      --config PG_USE_COPY YES \
      -t_srs EPSG:3005 \
      -f PostgreSQL \
      PG:"dbname=$dbname user=$dbuser host=$dbhost port=$dbport" \
      -lco OVERWRITE=YES \
      -lco SCHEMA=whse_basemapping \
      -lco GEOMETRY_NAME=geom \
      -nlt GEOMETRY \
      -nln $nln \
      "$tmp/FWA_BC.gdb" \
      $table
done

echo "Done."



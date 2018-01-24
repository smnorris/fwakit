# create test data


# test data too big to include in repo
mkdir test_data

fwakit dump -o test_data -g SALM -of FileGDB

cd test_data
zip -r FWA_BC.gdb.zip FWA_BC.gdb
zip -r FWA_LINEAR_BOUNDARIES_SP.gdb.zip FWA_LINEAR_BOUNDARIES_SP.gdb
zip -r FWA_STREAM_NETWORKS_SP.gdb.zip FWA_STREAM_NETWORKS_SP.gdb
zip -r FWA_WATERSHEDS_POLY.gdb.zip FWA_WATERSHEDS_POLY.gdb

scp *.zip snorris@hillcrestgeo.ca:/var/www/hillcrestgeo.ca/html/outgoing/fwakit

cd ..

# test data points for linear referencing - include in repo

# make sure pscis assessments are present in database
bc2pg pscis-assessments

# overlay with watershed groups and dump 10 SALM group points to shp
ogr2ogr \
  tests/data/pscis.shp \
  PG:'host=localhost user=postgres dbname=postgis password=postgres' \
  -sql "SELECT
          p.stream_crossing_id as pt_id,
          p.geom
        FROM whse_fish.pscis_assessment_svw p
        INNER JOIN whse_basemapping.fwa_watershed_groups_subdivided wsg
        ON ST_Intersects(p.geom, wsg.geom)
        WHERE wsg.watershed_group_code = 'SALM'
        LIMIT 100"


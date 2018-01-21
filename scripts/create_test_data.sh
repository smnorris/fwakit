# create test data

mkdir test_data

fwakit dump -o test_data -g COWN -of FileGDB

cd test_data
zip -r FWA_BC.gdb.zip FWA_BC.gdb
zip -r FWA_LINEAR_BOUNDARIES_SP.gdb.zip FWA_LINEAR_BOUNDARIES_SP.gdb
zip -r FWA_STREAM_NETWORKS_SP.gdb.zip FWA_STREAM_NETWORKS_SP.gdb
zip -r FWA_WATERSHEDS_POLY.gdb.zip FWA_WATERSHEDS_POLY.gdb

scp *.zip snorris@hillcrestgeo.ca:/var/www/hillcrestgeo.ca/html/outgoing/fwakit

cd ..
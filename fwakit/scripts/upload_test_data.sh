python create_test_data.py
zip -r FWA_BC.gdb.zip FWA_BC.gdb
zip -r FWA_STREAM_NETWORKS_SP.gdb.zip FWA_STREAM_NETWORKS_SP.gdb
rm -r FWA_STREAM_NETWORKS_SP.gdb
rm -r FWA_BC.gdb
scp FWA_STREAM_NETWORKS_SP.gdb.zip snorris@hillcrestgeo.ca:/var/www/hillcrestgeo.ca/html/fwakit
scp FWA_BC.gdb.zip snorris@hillcrestgeo.ca:/var/www/hillcrestgeo.ca/html/fwakit
rm FWA_STREAM_NETWORKS_SP.gdb.zip
rm FWA_BC.gdb.zip
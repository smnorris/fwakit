# somewhat minimize impact on storage when downloading FWA files and loading db
# (note that this does not load fwa_linear_boundaries_sp)

fwakit download -f FWA_BC.gdb.zip
fwakit load
rm -r fwakit_downloads/FWA_BC.gdb

fwakit download -f FWA_STREAM_NETWORKS_SP.gdb.zip
fwakit load
rm -r fwakit_downloads/FWA_STREAM_NETWORKS_SP.gdb.zip

fwakit download -f FWA_WATERSHEDS_POLY.gdb.zip
fwakit load
rm -r fwakit_downloads/FWA_WATERSHEDS_POLY.gdb
import os

import fwakit as fwa
from fwakit import watersheds
from fwakit.util import log

# set up the environment
os.environ["FWA_DB"] = r"postgresql://postgres:postgres@localhost:5432/postgis"

# input points
stations = r'data/stations.shp'

# load stations
log('Loading input points to postgres')
db = fwa.util.connect()
#db.ogr2pg(stations)

# reference stations to stream network
# keep just the closest matches - these points have been manually adjusted
log('Referencing input points to FWA streams within 100m')
fwa.reference_points(
    'public.stations',
    'station',
    'public.stations_referenced',
    100,
    closest=True,
    db=db)

# create preliminary watersheds (unaggregated first order watersheds)
# Note that this also creates wsdrefine_hexwsd and wsdrefine_streams for
# post processing the watersheds with DEM in ArcGIS
db['public.wsdrefine_prelim'].drop()
watersheds.points_to_watersheds(
    'public.stations_referenced',
    'station',
    'public.wsdrefine_prelim',
    db=db,
    dissolve=False)


# add USA watersheds
watersheds.add_ex_bc('public.stations',
                     'public.stations_referenced',
                     'station',
                     'public.wsdrefine_prelim')

# dump prelim watersheds to file for external processing
# (run DEM refinement with ArcGIS, aggregate with mapshaper)
db.pg2ogr(
    "SELECT station, waterbody_key AS wtrbdk, source, geom FROM public.wsdrefine_prelim",
    "ESRI Shapefile",
    "data/wsdrefine_prelim.shp"
)
db.pg2ogr(
    "SELECT * FROM public.wsdrefine_hexwsd",
    "ESRI Shapefile",
    "data/wsdrefine_hex.shp")
db.pg2ogr(
    "SELECT station, linear_feature_id AS lnrftrd, blue_line_key AS bllnk, geom "
    "FROM public.wsdrefine_streams",
    "ESRI Shapefile",
    "data/wsdrefine_streams.shp"
)
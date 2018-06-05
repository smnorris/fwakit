# note that before running this script a postgres instance holding the FWA
# and fwakit additions must be running on localhost

import os

import fwakit as fwa
from fwakit import watersheds
from fwakit.util import log

# set up the environment
os.environ["FWA_DB"] = r"postgresql://postgres:postgres@localhost:5432/postgis"
#os.environ["GDAL_DATA"] = r"E:\sw_nt\Python27\ArcGIS10.3\Lib\site-packages\osgeo\data\gdal"


# input data
#DEM = r'Q:\dsswhse\Data\Base\DEMs Hillshades\Base20\BC DEM.gdb\bc_dem'
#stations = r'Q:\dss_workarea\snorris\projects\streamflow_inventory\data\stations.shp'

DEM = r'/Volumes/Data/Data/BC/raster/dem/bc_dem.tif'
stations = r'/Volumes/Data/Projects/geobc/watershed_delineation/data/stations.shp'

# load stations
log('Loading input points to postgres')
db = fwa.util.connect()
db.ogr2pg(stations)

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
db['public.wsdrefine_prelim'].drop()
watersheds.points_to_watersheds(
    'public.stations_referenced',
    'station',
    'public.wsdrefine_prelim',
    db=db,
    dissolve=False)

# add any watershed area outside of BC
# (currently only USA lower 48 supported, and results will be of mixed quality)
watersheds.add_ex_bc(
    'public.stations_referenced',
    'station,',
    'public.wsdrefine_prelim'
)

# dump prelim watersheds to file to aggregate with mapshaper (faster than postgis/arc)
out_file = r'/Volumes/Data/Projects/geobc/watershed_delineation/wsdrefine_prelim.shp'
db.pg2ogr('SELECT * FROM public.wsdrefine_prelim', 'ESRI Shapefile', out_file)


"""

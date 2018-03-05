import os

import fwakit as fwa
from fwakit import watersheds
from fwakit import watersheds_arcgis
from fwakit.util import log

os.environ["FWA_DB"] = r"postgresql://postgres:postgres@localhost:5432/postgis"

# GTS data
os.environ["GDAL_DATA"] = r"E:\sw_nt\Python27\ArcGIS10.3\Lib\site-packages\osgeo\data\gdal"
DEM = r'Q:\dsswhse\Data\Base\DEMs Hillshades\Base20\BC DEM.gdb\bc_dem'
stations = r'Q:\dss_workarea\snorris\projects\streamflow_inventory\data\stations.shp'

# local dev data
#stations = r'/Volumes/Data/Projects/geobc/watershed_delineation/data/stations.shp'


# load stations
log('Creating watersheds')
db = fwa.util.connect()
db.ogr2pg(stations)

# reference to stream network
log('Referencing points to streams')
fwa.reference_points('public.stations', 'station',
                     'public.stations_streams', 100, db=db)

# keep just the closest matches - these points have been manually adjusted
log('Keeping just the points closest to a stream')
db['public.stations_referenced'].drop()
sql = """
   CREATE TABLE public.stations_referenced AS
   SELECT DISTINCT ON (station) *
   FROM public.stations_streams
   ORDER BY station, distance_to_stream
   """
db.execute(sql)

# create preliminary watersheds
db['public.stations_watersheds_prelim'].drop()
watersheds.points_to_watersheds('public.stations_referenced', 'station',
                                'public.stations_watersheds_prelim', db=db)

ref_table = 'public.stations_referenced'
ref_id = 'station'

# define output
db['public.test_wsdcut'].drop()
log('creating test_wsdcut')
db.execute("CREATE TABLE public.test_wsdcut (id serial, geom geometry)")


for station in db['public.stations_referenced']:
    ref_id_value = station['station']

    # get standard fwa linear referencing info about the location, plus
    # - whether it is on a double line river / canal
    # - downstream_route_measure of stream on which location lies
    (blue_line_key,
     pt_measure,
     wscode_ltree,
     localcode_ltree,
     waterbody_key,
     stream_measure) = watersheds.location_info(ref_table, ref_id, ref_id_value)

    # whether refining of the wateshed is required depends on whether the
    # point falls on a river/canal
    top_threshold_wb = 250
    bottom_threshold_wb = 1000
    top_threshold = 100
    bottom_threshold = 50

    if waterbody_key and (fwa.length_to_top_wsd(blue_line_key, pt_measure) > top_threshold_wb
                          and pt_measure > bottom_threshold_wb):
        log('Refining watershed - cutting at river -  {w}'.format(w=ref_id_value))
        sql = fwa.queries['wsdrefine_river_wsd_cut']
        sql = db.build_query(sql, {'ref_table': 'public.stations_referenced',
                                   'ref_id': 'station'})
        db.execute(sql, (ref_id_value,))
    if not waterbody_key and (fwa.length_to_top_wsd(blue_line_key, pt_measure) > top_threshold
                        and pt_measure > bottom_threshold):
        log('Refining watershed - using DEM -  {w}'.format(w=ref_id_value))

        # create hex cutout of watershed
        sql = db.build_query(fwa.queries['wsdrefine_hex_wsd'],
                             {'ref_table': 'public.stations_referenced',
                              'ref_id': 'station'})
        db.execute(sql, (ref_id_value))

        # extract stream upstream of the location
        sql = db.build_query(fwa.queries['wsdrefine_streams'],
                             {'ref_table': 'public.stations_referenced',
                              'ref_id': 'station'})
        db.execute(sql, (ref_id_value,))

        new_wsd = watersheds_arcgis.generate_new_wsd('wsdrefine_hex_wsd',
                                                     'wsdrefine_streams',
                                                     DEM,
                                                     db=db,
                                                     in_mem=False)
        # the watershed generated with the DEM can be a bit messy, instead of
        # using the result of vectorizing the raster watershed, select any
        # previously generated hex grid cells that intersect with the new
        # wsd definition
        sql = """CREATE TABLE wsdrefine_ref_wsd AW
                 SELECT h.geom
                 FROM wsdrefine_hex_wsd h
                 INNER JOIN wsdrefine_dem_wsd d
                 ON ST_Intersects(h.geom, d.geom)
              """
        db.execute(sql)
    else:
        log('Not refining watershed {w}'.format(w=ref_id_value))

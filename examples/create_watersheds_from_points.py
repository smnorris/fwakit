# note that before running this script a postgres instance holding the FWA
# and fwakit additions must be running on localhost

import os

import fwakit as fwa
from fwakit import watersheds
#from fwakit import watersheds_arcgis
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
log('Referencing input points to FWA streams within 100m')
fwa.reference_points('public.stations', 'station',
                     'public.stations_streams', 100, db=db)

# keep just the closest matches - these points have been manually adjusted
log('Retaining only the stream matches closest to a site')
db['public.stations_referenced'].drop()
sql = """
   CREATE TABLE public.stations_referenced AS
   SELECT DISTINCT ON (station) *
   FROM public.stations_streams
   ORDER BY station, distance_to_stream
   """
db.execute(sql)

ref_table = 'public.stations_referenced'
ref_id = 'station'

# create preliminary watersheds
db['public.wsdrefine_prelim'].drop()
watersheds.points_to_watersheds('public.stations_referenced', 'station',
                                'public.wsdrefine_prelim', db=db, dissolve=False)

# add a column that notes how the watershed was derived
db.execute("ALTER TABLE public.wsdrefine_prelim ADD COLUMN source text")
db.execute("UPDATE public.wsdrefine_prelim SET source = 'prelim'")

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
    # point falls on a river/canal. It isn't worth the complexity of refining
    # watersheds on waterbodies if the point is quite close to the edge.
    top_threshold_wb = 250
    bottom_threshold_wb = 100
    top_threshold = 100
    bottom_threshold = 50

    sql = fwa.queries['wsdrefine_length_to_top_bottom']
    sql = db.build_query(sql, {'ref_table': 'public.stations_referenced',
                               'ref_id': 'station'})
    (length_to_top, length_to_bottom) = db.query(sql, (ref_id_value,)).fetchone()

    # if on waterbody and within the set tolerances, cut the watersheds
    if waterbody_key and (length_to_top > top_threshold_wb
                          and length_to_bottom > bottom_threshold_wb):
        log('Refining watershed - cutting at river -  {w}'.format(w=ref_id_value))

        sql = fwa.queries['wsdrefine_river_wsd_cut']
        sql = db.build_query(sql, {'ref_table': 'public.stations_referenced',
                                   'ref_id': 'station'})
        db.execute(sql, (ref_id_value,))

    # If not on a waterbody and inside the set tolerances, refine wsd with DEM
    elif not waterbody_key and (length_to_top > top_threshold
                              and length_to_bottom > bottom_threshold):
        log('Refining watershed - using DEM -  {w}'.format(w=ref_id_value))

        # create hex cutout of watershed
        db['public.wsdrefine_hex_wsd'].drop()
        sql = db.build_query(fwa.queries['wsdrefine_hex_wsd'],
                             {'ref_table': 'public.stations_referenced',
                              'ref_id': 'station'})
        db.execute(sql, (ref_id_value))

        # extract stream upstream of the location
        db['public.wsdrefine_streams'].drop()
        sql = db.build_query(fwa.queries['wsdrefine_streams'],
                             {'ref_table': 'public.stations_referenced',
                              'ref_id': 'station'})
        db.execute(sql, (ref_id_value,))

        # use DEM to define area upstream of point within area of interest
        """
        watersheds_arcgis.generate_new_wsd('wsdrefine_hex_wsd',
                                           'wsdrefine_streams',
                                           DEM,
                                           db=db,
                                           in_mem=False)
        """
        # The watershed generated with the DEM can be messy. Instead of
        # using the result of vectorizing the raster watershed, select any
        # previously generated hex grid cells that intersect with the poly
        # defined by the DEM. This also ensures any unwanted tails below site
        # are removed
        sql = """INSERT INTO public.wsdrefine_prelim ({ref_id}, source, geom)
                 SELECT
                   h.{ref_id},
                   'DEM refined' as source,
                   ST_Multi(ST_Union(h.geom)) as geom
                 FROM public.wsdrefine_hex_wsd h
                 INNER JOIN public.wsdrefine_dem_wsd d
                 ON ST_Intersects(h.geom, d.geom)
                 GROUP BY h.{ref_id}
              """.format(ref_id=ref_id)
        #db.execute(sql)
    else:
        log('Not refining watershed {w}'.format(w=ref_id_value))
        # just insert the watershed where the point lies *as is*
        sql = fwa.queries['wsdrefine_norefine']
        sql = db.build_query(fwa.queries['wsdrefine_norefine'],
                             {'ref_table': 'public.stations_referenced',
                              'ref_id': 'station'})
        db.execute(sql, (ref_id_value,))

# dump results to file
out_file = r'/Volumes/Data/Projects/geobc/watershed_delineation/wsdrefine_prelim.shp'
db.pg2ogr('SELECT * FROM public.wsdrefine_prelim', 'ESRI Shapefile', out_file)

import os

import fwakit as fwa
from fwakit import watersheds
from fwakit.util import log

# set up the environment
os.environ["FWA_DB"] = r"postgresql://postgres:postgres@localhost:5432/postgis"

# input points
stations = r'data/stations.shp'

def job1():
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

def job2():
    """
    after dem watersheds are created in arc
    """
    # merge the watersheds
    # subprocess
    # merge_gdb_layers /Users/snorris/Dropbox/temp/fwa_temp.gdb -o data/wsdrefine_dem.shp
    # load dem watersheds to postgres
    #db.ogr2pg('data/wsdrefine_dem.shp', schema='public')
    #watersheds.add_wsdrefine_dem('public.wsdrefine_dem', 'public.wsdrefine_prelim')
    # dump to shapefile
    db.pg2ogr(
      "SELECT * FROM public.wsdrefine_prelim",
      driver="ESRI Shapefile",
      outfile="data/wsdrefine_mapshaper.shp"
    )
    #dissolve with mapshaper in subprocess
    log('Dissolving watersheds with mapshaper')
    #mapshaper data/wsdrefine_mapshaper.shp -dissolve station -o data/wsdrefine_prelimdiz.shp

    log('Loading watersheds to postgres')
    db = fwa.util.connect()
    db.ogr2pg('data/wsdrefine_prelimdiz.shp')

    # aggregate with st_union to remove overlaps
    # also, extract just outer ring to remove gaps and do minor buffering
    log('Removing overlaps')
    db['public.wsdrefine_agg'].drop()
    sql = """CREATE TABLE public.wsdrefine_agg AS
             SELECT station, st_union(ST_Buffer(ST_Buffer(geom, .001), -.001)) as geom
             FROM public.wsdrefine_prelimdiz
             GROUP BY station"""
    db.execute(sql)

    log('Clean up gaps. There may be some in border watersheds.')
    db['public.wsd'].drop()
    sql = """CREATE TABLE public.wsd AS
             SELECT
               station,
               ST_Collect(ST_MakePolygon(geom)) As geom
             FROM (SELECT
                     station,
                     ST_ExteriorRing((ST_Dump(geom)).geom) As geom
                   FROM public.wsdrefine_agg) as foo
             GROUP BY station
          """
    db.execute(sql)

    out_file =  r'data/stn_wsds.shp'
    db.pg2ogr('SELECT * FROM public.wsd',
              'ESRI Shapefile',
              out_file)


if __name__ == '__main__':
    job1()
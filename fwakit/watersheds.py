import os
import tempfile
try:
    from urllib.parse import urlparse
except ImportError:
     from urlparse import urlparse

import arcpy

import fwakit as fwa

def create_arcgis_db_connection(db_url=None, connection_file=None):
    if not db_url:
        db_url = os.environ['FWA_DB']
    if not connection_file:
        connection_file = os.path.join(tempfile.gettempdir(), 'fwa_db', 'fwakit.sde')
    if not os.path.exists(connection_file):
        out_path, out_file = os.path.split(connection_file)
        u = urlparse(db_url)
        database = u.path[1:]
        user = u.username
        password = u.password
        host = u.hostname
        port = u.port

        arcpy.CreateDatabaseConnection_management(
            out_path,
            out_file,
            'POSTGRESQL',
            host,
            'DATABASE_AUTH',
            user,
            password,
            'SAVE_USERNAME',
            database)
    return connection_file

def points_to_watersheds(in_table, in_id, out_table, dissolve=False, db=None):
    """
    Create a table holding watersheds upstream of the referenced locations
    provided. Input table must include fields:
       - unique id (in_id),
       - wscode_ltree
       - localcode_ltree
    """
    if not db:
        db = fwa.util.connect()
    sql = """
        CREATE TABLE {out_table} AS
        SELECT
          pt.{pk},
          pt.wscode_ltree as wscode_bottom,
          pt.localcode_ltree as localcode_bottom,
          wsd2.watershed_feature_id,
          wsd2.wscode_ltree,
          wsd2.localcode_ltree,
          wsd2.geom
        FROM {in_table} pt
        INNER JOIN whse_basemapping.fwa_watersheds_poly_sp wsd2
        ON
          -- b is a child of a, always
          wsd2.wscode_ltree <@ pt.wscode_ltree
        AND
            -- conditional upstream join logic, based on whether watershed codes are equivalent
          CASE
            -- first, consider simple case - streams where wscode and localcode are equivalent
             WHEN
                pt.wscode_ltree = pt.localcode_ltree
             THEN TRUE
             -- next, the more complicated case - where wscode and localcode are not equal
             WHEN
                pt.wscode_ltree != pt.localcode_ltree AND
                (
                 -- tributaries: b wscode > a localcode and b wscode is not a child of a localcode
                    (wsd2.wscode_ltree > pt.localcode_ltree AND
                     NOT wsd2.wscode_ltree <@ pt.localcode_ltree)
                    OR
                 -- capture side channels: b is the same watershed code, with larger localcode
                    (wsd2.wscode_ltree = pt.wscode_ltree
                     AND wsd2.localcode_ltree >= pt.localcode_ltree)
                )
              THEN TRUE
          END
          """.format(in_table=in_table, pk=in_id, out_table=out_table)
    db.execute(sql)
    db[out_table].create_index([in_id])
    db[out_table].create_index_geom()
    if dissolve:
        sql = """
              CREATE TEMPORARY TABLE temp_wsds_union AS
              SELECT
                {pk},
                wscode_bottom as wscode_ltree,
                localcode_bottom as localcode_ltree,
                ST_Union(geom)
              FROM {out_table}
              GROUP BY {pk}, wscode_bottom, localcode_bottom
              """.format(pk=in_id)
        db.execute(sql)
        db[out_table].drop()
        db.execute("CREATE TABLE {out_table} AS SELECT * FROM temp_wsds_union")
        # re-index the output
        db[out_table].create_index([in_id])
        db[out_table].create_index_geom()


def location_info(ref_table, ref_id, ref_id_value, db=None):
    """
    For provided table/id, return blue_line_key, measure, watershed codes,
    whether the location is on a double line river/canal and the measure at
    the bottom of the stream segement on which the location lies
    """
    if not db:
        db = fwa.util.connect()
    sql = """
            SELECT
              pts.blue_line_key,
              pts.downstream_route_measure,
              pts.wscode_ltree,
              pts.localcode_ltree,
              CASE
                WHEN riv.waterbody_key IS NOT NULL
                OR (mmwb.waterbody_key IS NOT NULL
                    AND mmwb.feature_code = 'GA03950000')
                THEN True
              END as waterbody,
              s.downstream_route_measure as stream_measure
            FROM {ref_table} pts
            LEFT JOIN LATERAL
             (SELECT
                blue_line_key,
                downstream_route_measure,
                waterbody_key,
                edge_type
              FROM whse_basemapping.fwa_stream_networks_sp
              WHERE
                blue_line_key = pts.blue_line_key
              AND downstream_route_measure <= pts.downstream_route_measure
              ORDER BY downstream_route_measure desc
              LIMIT 1
            ) s ON TRUE
            LEFT OUTER JOIN whse_basemapping.fwa_rivers_poly riv
            ON s.waterbody_key = riv.waterbody_key
            LEFT OUTER JOIN whse_basemapping.fwa_manmade_waterbodies_poly mmwb
            ON s.waterbody_key = mmwb.waterbody_key
            WHERE pts.{ref_id} = %s
        """.format(ref_table=ref_table, ref_id=ref_id)
    return db.query(sql, (ref_id_value)).fetchone()


def generate_new_wsd(wsd_in, streams, wsd_out, dem):
    """
    Refine a watershed polygon, cutting the bottom boundary where water flows
    to the bottom of the supplied streams layer.

    - wsd_in: area to be refined, supplied by derive_refine_watershed
    - streams: streams flowing through watershed, terminating at the point
      at which we want the new watershed polygon to end
    - wsd_out: output feature class
    """

    if arcpy.CheckExtension("Spatial") == "Available":
        arcpy.CheckOutExtension("Spatial")
    else:
        raise EnvironmentError('Spatial Analyst license unavailable')

    #arcpy.env.workspace = "IN_MEMORY"
    arcpy.env.workspace = r"T:\test\t.gdb"
    arcpy.env.overwriteOutput = True
    arcpy.env.extent = "MAXOF"

    # make required feature layers of input watersheds and streams
    arcpy.MakeFeatureLayer_management(streams, 'streams_fl')
    arcpy.MakeFeatureLayer_management(wsd_in, 'wsd_fl')

    # convert streams to raster
    if arcpy.Exists('streams_pourpt'):
        arcpy.Delete_management('streams_pourpt')
    arcpy.FeatureToRaster_conversion('streams_fl', 'blue_line_key',
                                     'streams_pourpt', '25')
    print 'clipping dem'
    # clip DEM to extent of watershed + 250m
    extent = arcpy.Describe(wsd_in).extent
    expansion = 250
    xmin = extent.XMin - expansion
    ymin = extent.YMin - expansion
    xmax = extent.XMax + expansion
    ymax = extent.YMax + expansion
    envelope = (xmin, ymin, xmax, ymax)
    rectangle = " ".join([str(e) for e in envelope])
    arcpy.Clip_management(dem, rectangle, 'dem_wsd')

    # output rasters clipped to input wsd poly by creating a mask
    arcpy.env.mask = 'wsd_fl'
    print 'filling'
    # fill the dem, calculate flow direction and create watershed raster
    fill = arcpy.sa.Fill('dem_wsd')
    flow_direction = arcpy.sa.FlowDirection(fill, 'NORMAL')
    wsd_grid = arcpy.sa.Watershed(flow_direction, 'streams_pourpt')

    # check to make sure there is a result - if all output raster is null,
    # do not try to create a watershed polygon output
    out_is_null = arcpy.sa.IsNull(wsd_grid)
    check_min_result = arcpy.GetRasterProperties_management(out_is_null, "MINIMUM")
    check_min = check_min_result.getOutput(0)
    check_max_result = arcpy.GetRasterProperties_management(out_is_null,"MAXIMUM")
    check_max = check_max_result.getOutput(0)
    if '0' in (check_min, check_max):
        arcpy.RasterToPolygon_conversion(wsd_grid, wsd_out, "SIMPLIFY")


def refine_watershed(ref_table, ref_id, ref_id_value, prelim_wsd_table,
                     dem, top_threshold=100, bottom_threshold=50, db=None):
    """
    Based on provided table and id, refine preliminary watershed for that
    location if requred
    Refining of a watershed is required if:
     - location is on double line river
     - location is more than provided threshold values from top/bottom of wsd
    """
    if not db:
        db = fwa.util.connect()

    # create arcgis connection file pointing to db
    arcgis_db = create_arcgis_db_connection(db.url)

    # get standard fwa linear referencing info about the location, plus
    # - whether it is on a double line river / canal
    # - downstream_route_measure of stream on which location lies
    (blue_line_key,
     pt_measure,
     wscode_ltree,
     localcode_ltree,
     on_river_flag,
     stream_measure) = location_info(ref_table, ref_id, ref_id_value)

    # determine if refining of watershed is required
    if on_river_flag or (fwa.length_to_top_wsd(blue_line_key,
                                               pt_measure) > top_threshold and
                         pt_measure > bottom_threshold):

        # extract all stream upstream of point
        sql = fwa.queries['select_upstream_geom']
        q = db.mogrify(sql, (pt_measure, blue_line_key, pt_measure))

        arcpy.MakeQueryLayer_management(
            arcgis_db,
            'streams',
            q,
            'linear_feature_id')

        # extract area to be refined. The area used is simply the primary
        # watershed in which the point lies for most cases, but when the location
        # is referenced to a double line river/canal we need to use adjacent
        # non-waterbody watershed polygons
        if on_river_flag:
            # get adjacent watersheds / share a boundary - see these for
            # ST_Relate documentation:
            # - https://postgis.net/docs/using_postgis_dbmanagement.html#DE-9IM)
            # - http://postgis.17.x6.nabble.com/ST-Touches-and-line-segment-td3562507.html
            sql = """
                   SELECT DISTINCT wsd.watershed_feature_id, wsd.geom
                   FROM {prelim_table} p
                   INNER JOIN whse_basemapping.fwa_watersheds_poly_sp wsd
                   ON (p.geom && wsd.geom
                   AND ST_Relate(p.geom, wsd.geom, '****1****'))
                   WHERE p.{ref_id} = %s
                   AND p.wscode_ltree = %s
                   AND p.localcode_ltree = %s
                   AND wsd.waterbody_key = 0
                  """.format(prelim_table=prelim_wsd_table,
                             ref_id=ref_id)
            q = db.mogrify(sql, (ref_id_value, wscode_ltree, localcode_ltree))
            arcpy.MakeQueryLayer_management(
                arcgis_db,
                'wsd_to_refine',
                q,
                'watershed_feature_id')

        else:
            # if referenced location is not on a river/canal, just extract
            # watersheds with equivalent watershed codes
            sql = """
                  SELECT DISTINCT wsd.watershed_feature_id, wsd.geom
                   FROM whse_basemapping.fwa_watersheds_poly_sp wsd
                   WHERE p.wscode_ltree = %s
                   AND p.localcode_ltree = %s
                   AND wsd.waterbody_key = 0
                  """
            q = db.mogrify(sql, (wscode_ltree, localcode_ltree))
            arcpy.MakeQueryLayer_management(
                arcgis_db,
                'wsd_to_refine',
                q,
                'watershed_feature_id')
        generate_new_wsd('wsd_to_refine', 'streams', r'T:\test\wsd.shp', dem)
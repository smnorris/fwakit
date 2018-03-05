import os
import tempfile
try:
    from urllib.parse import urlparse
except ImportError:
     from urlparse import urlparse

import arcpy

import fwakit as fwa
from fwakit.util import log


def create_wksp(path, gdb):
    """ Create a .gdb workspace in given path
    """
    wksp = os.path.join(path, gdb)
    if not arcpy.Exists(wksp):
        arcpy.CreateFileGDB_management(path, gdb)
    return os.path.join(path, gdb)


def create_arcgis_db_connection(db_url=None, connection_file=None):
    if not db_url:
        db_url = os.environ['FWA_DB']
    if not connection_file:
        connection_file = os.path.join(tempfile.gettempdir(),
                                       'fwakit',
                                       'fwakit.sde')
    if not os.path.exists(connection_file):
        out_path, out_file = os.path.split(connection_file)
        u = urlparse(db_url)
        database = u.path[1:]
        user = u.username
        password = u.password
        host = u.hostname
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


def generate_new_wsd(wsdrefine_hex, wsdrefine_streams,
                     dem, db=None, in_mem=True):
    """
    Refine a watershed polygon, cutting the bottom boundary where water flows
    to the bottom of the supplied streams layer.

    - wsdrefine_hex:     watershed area to be refined, as a hex-grid
    - wsdrefine_streams: stream upstream of the location at which to terminate
                         the watershed
    """
    # set a spot to write temp output(s)
    temp_wksp = os.path.join(tempfile.gettempdir(), 'fwakit', 'fwa_temp.gdb')
    p, f = os.path.split(temp_wksp)
    temp_wksp = create_wksp(p, f)

    # get spatial analyst and set env
    if arcpy.CheckExtension("Spatial") == "Available":
        arcpy.CheckOutExtension("Spatial")
    else:
        raise EnvironmentError('Spatial Analyst license unavailable')
    arcpy.env.overwriteOutput = True
    arcpy.env.extent = "MAXOF"
    if in_mem:
        arcpy.env.workspace = "IN_MEMORY"
    else:
        arcpy.env.workspace = temp_wksp

    # create db connections
    if not db:
        db = fwa.util.connect()
    arcgis_db = create_arcgis_db_connection(db.url)

    # make required feature layers of input watersheds and streams
    arcpy.MakeQueryLayer_management(
        arcgis_db,
        'streams_ql',
        'SELECT * FROM wsdrefine_streams',
        'linear_feature_id')
    arcpy.MakeQueryLayer_management(
        arcgis_db,
        'hex_wsd_ql',
        'SELECT * FROM wsdrefine_hex_wsd',
        'linear_feature_id')

    # convert streams to raster
    if arcpy.Exists('streams_pourpt'):
        arcpy.Delete_management('streams_pourpt')
    arcpy.FeatureToRaster_conversion('streams_ql', 'blue_line_key',
                                     'streams_pourpt', '25')

    # clip DEM to extent of watershed + 250m
    log('Refining watershed - extracting DEM')
    extent = arcpy.Describe('hex_wsd_ql').extent
    expansion = 250
    xmin = extent.XMin - expansion
    ymin = extent.YMin - expansion
    xmax = extent.XMax + expansion
    ymax = extent.YMax + expansion
    envelope = (xmin, ymin, xmax, ymax)
    rectangle = " ".join([str(e) for e in envelope])
    arcpy.Clip_management(dem, rectangle, 'dem_wsd')

    # output rasters clipped to input wsd poly by creating a mask
    arcpy.env.mask = 'hex_wsd_ql'

    # fill the dem, calculate flow direction and create watershed raster
    log('Refining watershed - filling DEM')
    fill = arcpy.sa.Fill('dem_wsd')
    flow_direction = arcpy.sa.FlowDirection(fill, 'NORMAL')
    wsd_grid = arcpy.sa.Watershed(flow_direction, 'streams_pourpt')

    # check to make sure there is a result - if all output raster is null,
    # do not try to create a watershed polygon output
    log('Refining watershed - creating new watershed from DEM and streams')
    out_is_null = arcpy.sa.IsNull(wsd_grid)
    check_min_result = arcpy.GetRasterProperties_management(out_is_null,
                                                            "MINIMUM")
    check_min = check_min_result.getOutput(0)
    check_max_result = arcpy.GetRasterProperties_management(out_is_null,
                                                            "MAXIMUM")
    check_max = check_max_result.getOutput(0)
    if '0' in (check_min, check_max):
        arcpy.RasterToPolygon_conversion(wsd_grid,
                                         os.path.join(temp_wksp, 'wsd_new'),
                                         "SIMPLIFY")
    # load result to postgres db
    db['public.wsd_new'].drop()
    db.ogr2pg(temp_wksp, 'wsd_new')

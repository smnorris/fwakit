import os
import tempfile
try:
    from urllib.parse import urlparse
except ImportError:
     from urlparse import urlparse

import arcpy

import bcdata

from fwakit.util import log


def create_wksp(path, gdb):
    """ Create a .gdb workspace in given path
    """
    if not os.path.exists(path):
        os.makedirs(path)
    wksp = os.path.join(path, gdb)
    if not arcpy.Exists(wksp):
        arcpy.CreateFileGDB_management(path, gdb)
    return os.path.join(path, gdb)


def wsdrefine_dem(in_wsd, in_streams, wsd_id, in_mem=True):
    """
    Refine a layer of watershed polygons, cutting the bottom boundary where water flows
    to the bottom of the supplied streams layer.

    - in_wsd:  feature class holding watershed areas to be refined
    - in_streams: stream upstream of the location at which to terminate the watersheds
    - wsd_id:  unique id for watershed, present in in_wsd and in_streams
    """
    # set a spot to write temp output(s)
    temp_folder = os.path.join(tempfile.gettempdir(), 'fwakit')
    temp_wksp = os.path.join(temp_folder, 'fwa_temp.gdb')
    p, f = os.path.split(temp_wksp)
    temp_wksp = create_wksp(p, f)

    # get spatial analyst and set env
    if arcpy.CheckExtension("Spatial") == "Available":
        arcpy.CheckOutExtension("Spatial")
    else:
        raise EnvironmentError('Spatial Analyst license unavailable')

    if in_mem:
        arcpy.env.workspace = "IN_MEMORY"
    else:
        arcpy.env.workspace = temp_wksp


    arcpy.env.overwriteOutput = True
    #arcpy.env.extent = "MAXOF"

    # get list of distinct watersheds
    # (there may be more than one poly per watershed)
    distinct_ids = sorted(list(set(row[0] for row in arcpy.da.SearchCursor(in_wsd, wsd_id))))

    # loop through the distinct watersheds
    for wsd_id_value in distinct_ids:
        log('Refining watershed %s' % str(wsd_id_value))

        # reset env.extent
        extent = arcpy.Describe(in_wsd).extent
        arcpy.env.extent = extent

        arcpy.MakeFeatureLayer_management(
            in_streams,
            'streams_fl',
            '"{}" = \'{}\''.format(wsd_id, wsd_id_value)
        )
        arcpy.MakeFeatureLayer_management(
            in_wsd,
            'wsd_fl', '"{}" = \'{}\''.format(wsd_id, wsd_id_value)
        )

        log('"{}" = \'{}\''.format(wsd_id, wsd_id_value))
        log('  - writing wsd to temp fc')

        # write the watershed to a feature class so we can get the extent
        # and create mask
        arcpy.Dissolve_management(
            'wsd_fl',
            'wsd_fc_tmp',
            wsd_id
        )

        # set extent to wsd polygon
        arcpy.env.mask = 'wsd_fc_tmp'
        extent = arcpy.Describe('wsd_fc_tmp').extent
        arcpy.env.extent = extent

        # convert streams to raster
        log('  - writing streams to raster')
        if arcpy.Exists('streams_pourpt'):
            arcpy.Delete_management('streams_pourpt')
        arcpy.FeatureToRaster_conversion('streams_fl', 'bllnk',
                                         'streams_pourpt', '25')

        # get DEM
        log('  - extracting DEM')
        expansion = 250
        xmin = extent.XMin - expansion
        ymin = extent.YMin - expansion
        xmax = extent.XMax + expansion
        ymax = extent.YMax + expansion
        bounds = (xmin, ymin, xmax, ymax)
        #rectangle = " ".join([str(e) for e in envelope])
        #log(rectangle)
        #arcpy.Clip_management(dem, rectangle, 'dem_wsd')
        bcdata.get_dem(bounds, os.path.join(temp_folder, "dem_wsd.tif"))
        # fill the dem, calculate flow direction and create watershed raster
        log('  - filling DEM')
        fill = arcpy.sa.Fill(os.path.join(temp_folder, "dem_wsd.tif"), 100)
        #fill.save(r"T:\fwakit\fl_"+wsd_id_value)
        log('  - calculating flow direction')
        flow_direction = arcpy.sa.FlowDirection(fill, 'NORMAL')
        #flow_direction.save(r"T:\fwakit\fd_"+wsd_id_value)
        log('  - creating DEM based watershed')
        wsd_grid = arcpy.sa.Watershed(flow_direction, 'streams_pourpt')
        # check to make sure there is a result - if all output raster is null,
        # do not try to create a watershed polygon output
        out_is_null = arcpy.sa.IsNull(wsd_grid)
        check_min_result = arcpy.GetRasterProperties_management(out_is_null,
                                                                "MINIMUM")
        check_min = check_min_result.getOutput(0)
        check_max_result = arcpy.GetRasterProperties_management(out_is_null,
                                                                "MAXIMUM")
        check_max = check_max_result.getOutput(0)
        if '0' in (check_min, check_max):
            out_fc = os.path.join(temp_wksp, 'wsd_dem_'+str(wsd_id_value))
            log('  - writing new watershed to %s' % out_fc )
            arcpy.RasterToPolygon_conversion(
                wsd_grid,
                out_fc,
                "SIMPLIFY")

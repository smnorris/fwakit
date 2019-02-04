import json
import logging as lg

from sqlalchemy import text

from skimage.morphology import skeletonize
from scipy import ndimage
import numpy as np

import geojson
import pyproj
import fiona
from rasterio import features
from shapely import geometry, ops
from pysheds.grid import Grid

import bcdata
import fwakit as fwa
from fwakit import epa_waters
from fwakit.util import log


def filter_bounds(in_file, prop, val):
    with fiona.open(in_file) as src:
        filtered = filter(lambda f: f['properties'][prop]==val, src)
        xs = []
        ys = []
        for j, feat in enumerate(filtered):
            w, s, e, n = fiona.bounds(feat)
            xs.extend([w, e])
            ys.extend([s, n])
        w, s, e, n = (min(xs), min(ys), max(xs), max(ys))
        return[w, s, e, n]


def points_to_watersheds(ref_table, ref_id, out_table, dissolve=False, db=None):
    """
    Create a table holding watersheds upstream of the referenced locations
    provided. Input ref_table table must include fields:
       - unique id (ref_id),
       - wscode_ltree
       - localcode_ltree

    Note: this could perhaps be sped up significantly by using the pre-aggregated
    assessment watersheds as the first step of selection rather than building
    everything from scratch.
    """
    # lower case ids only
    ref_id = ref_id.lower()
    # first, collect first order watersheds upstream of points
    points_to_prelim_watersheds(ref_table, ref_id, out_table)

    # add the first order watersheds on which the points lie (and refine if necessary)
    add_local_watersheds(ref_table, ref_id, out_table)

    # Dissolve if specified
    if dissolve:
        # Note that removing the interior linework of the watersheds is
        # *extremely* slow with ST_Union(geom) or with ST_Buffer(ST_Collect(geom))
        # We could use some other tool (mapshaper) to run the aggregation, but
        # Brad Sparks used this neat trick to remove need for aggregation - clip
        # the province with the watershed. Rather than try and do it all at once,
        # iterate through each station. This could be sped up even more by running
        # the watersheds in parallel.
        # create temp output
        sql = """CREATE TEMPORARY TABLE wsd_agg
                 (LIKE {out_table})
              """.format(out_table=out_table)
        db.execute(sql)
        # get ids to iterate through
        sql = """SELECT DISTINCT {id}
                 FROM {ref_table}
                 ORDER BY {id}
               """.format(id=ref_id,
                          ref_table=ref_table)
        for record in db.query(sql).fetchall():
            site = record[ref_id]
            log('Aggregating '+str(site))
            # run the clip/intersect
            sql = """INSERT INTO wsd_agg
                     SELECT {ref_id},
                       CASE WHEN ST_Within(a.geom, b.geom) THEN a.geom
                            ELSE ST_Intersection(a.geom, b.geom)
                       END as geom
                    FROM whse_basemapping.fwa_watershed_groups_subdivided a
                    INNER JOIN {out_table} b
                    ON ST_Intersects(a.geom, b.geom)
                    WHERE {ref_id} = %s
              """.format(ref_id=ref_id, out_table=out_table)
            db.execute(sql, (site,))
        # move the aggregated data over into the output table
        db[out_table].drop()
        db.execute("""CREATE TABLE {out_table} AS
                      SELECT * FROM wsd_agg
                   """.format(out_table=out_table))
        # re-index the output
        db[out_table].create_index([ref_id])
        db[out_table].create_index_geom()


def points_to_prelim_watersheds(ref_table, ref_id, out_table, dissolve=False, db=None):
    log('Creating %s, first order watersheds upstream of locations in %s' %
        (out_table, ref_table)
    )
    if not db:
        db = fwa.util.connect()
    # lower case ids only
    ref_id = ref_id.lower()
    # We could wrap all of the sql below that collects the watershed polys into one
    # query using CTE, but unfortunately performance degrades - probably due to this:
    # https://blog.2ndquadrant.com/postgresql-ctes-are-optimization-fences/
    # Nested subquery performance was not good either, so lets create a temporary
    # table of prelim upstream watersheds (noting lakes and reservoirs) and then do
    # any required additions afterwards
    sql = """
        CREATE TEMPORARY TABLE temp_prelim_wsds AS
        SELECT
          pt.{pk},
          wsd.watershed_feature_id,
          wsd.waterbody_key,
    -- note components of lakes and reservoirs
          CASE
            WHEN l.waterbody_key IS NOT NULL OR wb.waterbody_key IS NOT NULL
            THEN 'wb'
          END AS waterbody_ind,
          ST_Multi(ST_Force2D(wsd.geom)) as geom
        FROM {ref_table} pt
        INNER JOIN whse_basemapping.fwa_watersheds_poly_sp wsd
        ON
          -- b is a child of a, always
          wsd.wscode_ltree <@ pt.wscode_ltree
          -- don't include the bottom watershed
        AND wsd.localcode_ltree != pt.localcode_ltree
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
                    (wsd.wscode_ltree > pt.localcode_ltree AND
                     NOT wsd.wscode_ltree <@ pt.localcode_ltree)
                    OR
    -- capture side channels: b is the same watershed code, with larger localcode
                    (wsd.wscode_ltree = pt.wscode_ltree
                     AND wsd.localcode_ltree >= pt.localcode_ltree)
                )
              THEN TRUE
          END
        LEFT OUTER JOIN whse_basemapping.fwa_lakes_poly l
        ON wsd.waterbody_key = l.waterbody_key
        LEFT OUTER JOIN whse_basemapping.fwa_manmade_waterbodies_poly wb
        ON wsd.waterbody_key = wb.waterbody_key
    """.format(ref_table=ref_table, pk=ref_id)
    db.execute(sql)
    # The above prelim query selects all watershed polygons with watershed codes
    # greater than the codes of the poly in which the point lies. In some cases
    # this is not enough, additional polygons with equivalent watershed codes
    # are adjacent to the watershed in which the points lie.
    # For example, consider a point just downstream of Pinantan Lake on Paul Creek.
    # The bottom two watershed polys in the lake have the same watershed code
    # as Paul Creek downstream of the lake - therefore they do not get included in
    # a prelim wsd
    # SO - Ensure that the entire waterbody is included below

    # index the temp table
    db.execute("CREATE INDEX ON temp_prelim_wsds (waterbody_key)")

    # create output table
    sql = """CREATE TABLE {out_table} AS
    SELECT {pk}, watershed_feature_id, waterbody_key, geom
    FROM temp_prelim_wsds
    UNION
    SELECT
      lr.{pk},
      w.watershed_feature_id,
      lr.waterbody_key,
      ST_Multi(ST_Force2D(w.geom)) AS geom
    FROM
      (SELECT DISTINCT p.{pk}, p.waterbody_key
       FROM temp_prelim_wsds p
       WHERE waterbody_ind IS NOT NULL
      ) AS lr
    INNER JOIN whse_basemapping.fwa_watersheds_poly_sp w
    ON lr.waterbody_key = w.waterbody_key
    """.format(out_table=out_table, pk=ref_id)
    db.execute(sql)
    db[out_table].create_index([ref_id])
    db[out_table].create_index_geom()

    # add a column that notes how the watershed was derived
    db.execute("""ALTER TABLE {out_table} ADD COLUMN source text
               """.format(out_table=out_table))
    db.execute("""UPDATE {out_table} SET source = 'fwa_watersheds_poly_sp'
               """.format(out_table=out_table))


def get_refine_method(fwa_point_event, db=None):
    """
    Whether refining of the bottom first order wateshed is required depends on whether
    the location falls on a river/canal. It isn't worth the complexity of refining
    watersheds on waterbodies if the point is quite close to the edge.
    """
    refinement_thresholds = {
        'top': 100,
        'top_with_waterbody': 0,
        'bottom': 50,
        'bottom_with_waterbody': 0
    }
    if not db:
        db = fwa.util.connect()
    # is the point on a lake, river or reservoir/canal?
    sql = """SELECT
              CASE
                WHEN wb.waterbody_type IN ('L', 'R')
                  THEN s.waterbody_key
                WHEN wb.waterbody_type = 'X' AND wb.feature_code = 'GA03950000'
                  THEN s.waterbody_key
              END AS waterbody_key
            FROM whse_basemapping.fwa_stream_networks_sp s
            LEFT OUTER JOIN whse_basemapping.fwa_waterbodies wb
            ON s.waterbody_key = wb.waterbody_key
            WHERE linear_feature_id = %s"""
    waterbody_key = db.query(sql, fwa_point_event['linear_feature_id']).fetchone()[0]
    sql = fwa.queries['wsdrefine_length_to_top_bottom']
    sql = text(sql)
    length_to_top, length_to_bottom = db.engine.execute(
        sql,
        blue_line_key=fwa_point_event['blue_line_key'],
        downstream_route_measure=fwa_point_event['downstream_route_measure'],
        linear_feature_id=fwa_point_event['linear_feature_id'],
        wscode_ltree=str(fwa_point_event['wscode_ltree']),
        localcode_ltree=str(fwa_point_event['localcode_ltree'])
    ).fetchone()
    log("l_top: %s, l_bottom %s" % (length_to_top, length_to_bottom), level=lg.DEBUG)
    # modify the thresholds if on a waterbody
    if waterbody_key:
        refinement_thresholds['top'] = refinement_thresholds['top_with_waterbody']
        refinement_thresholds['bottom'] = refinement_thresholds['bottom_with_waterbody']

    # first, if the point is less than threshold to top, do not include the watershed
    if (length_to_top < refinement_thresholds['top']):
        return 'DROP'
    # if the point is less than threshold to bottom, do not attempt to refine
    elif length_to_bottom < refinement_thresholds['bottom']:
        return None
    # if we are refining, refine based on whether or not point is on waterbody
    elif waterbody_key:
        return 'CUT'
    elif not waterbody_key:
        return 'DEM'


def add_local_watersheds(ref_table, ref_id, prelim_wsd_table, db=None):
    """
    Insert boundary of the first order watershed in which a point lies.
    The first order watershed will be 'refined' if the point is not
    within the given distance tolerances from adjacent confluences.
    Refining is done with the DEM if the watershed is defined/drained by a single line
    stream. If the watershed is drained/defined by a waterbody (river/lake etc), the
    watershed is cut at the line defined by the closest point on each side of the
    waterbody to the point location in the waterbody.
    """
    # lower case ids only
    ref_id = ref_id.lower()

    log('Adding first order watershed in which points lie to %s' %
        (prelim_wsd_table)
    )
    if not db:
        db = fwa.util.connect()

    # create intermediate tables, required so that we can process all the DEM refinements
    # in arcgis separately. Ensure the primary key is the right type by selecting from
    # source table.
    db['public.wsdrefine_cut'].drop()
    db['public.wsdrefine_hexwsd'].drop()
    db['public.wsdrefine_streams'].drop()
    sql = """CREATE TABLE public.wsdrefine_cut AS
             SELECT
               {ref_id},
               NULL::geometry(MultiPolygon, 3005) as geom
             FROM {ref_table} LIMIT 0
          """.format(ref_id=ref_id,
                     ref_table=ref_table)
    db.execute(sql)
    sql = """CREATE TABLE public.wsdrefine_hexwsd AS
             SELECT
               {ref_id},
               NULL::geometry(MultiPolygon, 3005) as geom
             FROM {ref_table} LIMIT 0
          """.format(ref_id=ref_id,
                     ref_table=ref_table)
    db.execute(sql)
    sql = """CREATE TABLE public.wsdrefine_streams AS
             SELECT
               {ref_id},
               NULL::integer as linear_feature_id,
               NULL::integer as blue_line_key,
               NULL::geometry(MultiLineString, 3005) as geom
             FROM {ref_table} LIMIT 0
          """.format(ref_id=ref_id,
                     ref_table=ref_table)
    db.execute(sql)
    for fwa_point_event in db[ref_table]:
        ref_id_value = fwa_point_event[ref_id]
        refine_method = get_refine_method(fwa_point_event)

        # If watershed is on a waterbody and inside our distance tolerances, cut it
        if refine_method == 'CUT':
            log('Site {w}: refining watershed - cutting at river'.format(w=ref_id_value))

            # cut the polys
            sql = fwa.queries['wsdrefine_river_wsd_cut']
            sql = db.build_query(sql, {'ref_table': ref_table,
                                       'ref_id': ref_id})
            db.execute(sql, (ref_id_value,))

            # remove the polys that have been cut from the prelim wsd table,
            # there are more than just the poly in which the point lies
            sql = fwa.queries['wsdrefine_river_wsd_cut_remove']
            sql = db.build_query(sql, {'ref_table': ref_table,
                                       'ref_id': ref_id,
                                       'prelim': prelim_wsd_table})
            db.execute(sql, (ref_id_value, ref_id_value))
            # check that something valid was created, if the split was unsuccessful use DEM
            sql = """
                  SELECT {id}
                  FROM public.wsdrefine_cut
                  WHERE {id} = %s
                  AND ST_IsValid(geom)""".format(id=ref_id)
            result = db.query(sql, (ref_id_value)).fetchone()
            if not result[0]:
                log('Site {w}: could not cut at river, using DEM')
                refine_method = 'DEM'

        # If not on a waterbody and inside our distance tolerances, refine wsd with DEM
        # to make processing later with arcgis easier, just generate the inputs required
        if refine_method == 'DEM':
            log('Site {w}: prepping to refine with DEM'.format(w=ref_id_value))

            # create hex cutout of watershed
            sql = db.build_query(fwa.queries['wsdrefine_hexwsd'],
                                 {'ref_table': ref_table,
                                  'ref_id': ref_id})
            db.execute(sql, (ref_id_value))

            # extract stream upstream of the location
            sql = db.build_query(fwa.queries['wsdrefine_streams'],
                                 {'ref_table': ref_table,
                                  'ref_id': ref_id})
            db.execute(sql, (ref_id_value,))

        elif refine_method is None:
            log('Site {w}: inserting unrefined 1st order watershed'.format(w=ref_id_value))
            # just insert the watershed where the point lies *as is*
            sql = fwa.queries['wsdrefine_norefine']
            sql = db.build_query(fwa.queries['wsdrefine_norefine'],
                                 {'ref_table': ref_table,
                                  'ref_id': ref_id,
                                  'out_table': prelim_wsd_table})
            db.execute(sql, (ref_id_value,))

        elif refine_method == 'DROP':
            # nothing to do
            log('Site {w}: not inserting 1st order watershed'.format(w=ref_id_value))


def wsdrefine_dem(in_wsds, in_streams, in_points, ref_id):
    """Refine provided watersheds by using DEM
    """
    # find unique IDs in hex watershed shapefile

    with fiona.open(in_wsds) as src:
        id_type = src.schema['properties'][ref_id]
        stations = sorted(list(set([f['properties'][ref_id] for f in src])))

        schema = {
            'geometry': 'Polygon',
            'properties': {ref_id: id_type}
        }

        # create destination shapefile and open
        with fiona.open('data/wsdrefine_dem.shp', 'w', driver='ESRI Shapefile', crs=src.crs, schema=schema) as dst:
            # loop through each watershed, insert new catchment
            for station_id in stations:
                log("Refining watershed for point {}".format(station_id))
                bounds = filter_bounds(in_wsds, ref_id, station_id)
                pourpoint_coord = filter_bounds(in_points, ref_id, station_id)[0:2]
                # more than one polygon can be returned (and rasterio.shapes does
                # not return multipolygons - dump each to file and handle
                # aggregation elsewhere because some features seem to crash python
                # even when valid
                catchment_polys = create_catchment(ref_id, station_id, pourpoint_coord, bounds)
                for catchment in catchment_polys:
                    rec = {}
                    rec['geometry'] = geometry.mapping(catchment)
                    rec['id'] = str(station_id)
                    rec['properties'] = {ref_id: station_id}
                    dst.write(rec)


def create_catchment(id_column, id_value, pourpoint_coord, bounds):
    """Delineate catchment within provided bounds, upstream of provided point
    """

    # expand provided bounds by 250m on each side
    expansion = 250
    xmin = bounds[0] - expansion
    ymin = bounds[1] - expansion
    xmax = bounds[2] + expansion
    ymax = bounds[3] + expansion
    expanded_bounds = (xmin, ymin, xmax, ymax)

    bcdata.get_dem(expanded_bounds, "dem.tif")

    grid = Grid.from_raster("dem.tif", data_name='dem')

    # load FWA streams within area of interest and rasterize
    with fiona.open("data/wsdrefine_streams.shp") as src:
        stream_features = list(filter(lambda f: f['properties'][id_column] == id_value, src))

    # convert stream geojson features to shapely shapes
    stream_shapes = [geometry.shape(f['geometry']) for f in stream_features]

    # convert shapes to raster
    stream_raster = features.rasterize(
        ((g, 1) for g in stream_shapes),
        out_shape=grid.shape,
        transform=grid.affine,
        all_touched=False
    )
    stream_raster = skeletonize(stream_raster).astype(np.uint8)

    # Create boolean mask based on rasterized river shapes
    mask = stream_raster.astype(np.bool)

    # Create a view onto the DEM array
    dem = grid.view('dem', dtype=np.float64, nodata=np.nan)

    # Blur mask using a gaussian filter
    blurred_mask = ndimage.filters.gaussian_filter(mask.astype(np.float64), sigma=2.5)

    # Set central channel to max to prevent pits
    blurred_mask[mask.astype(np.bool)] = blurred_mask.max()

    # Set elevation change for burned cells
    dz = 16.5

    # Set mask to blurred mask
    mask = blurred_mask

    # Create a view onto the DEM array
    dem = grid.view('dem', dtype=np.float64, nodata=np.nan)

    # Subtract dz where mask is nonzero
    dem[(mask > 0)] -= dz*mask[(mask > 0)]

    # Smooth the mask in an effort to make sure the DEM goes downhill
    # where the stream is present
    #dem[(mask > 0)] = ndimage.filters.minimum_filter(dem, 2)[(mask > 0)]

    # defining the crs used improves results
    new_crs = pyproj.Proj('+init=epsg:3005')

    #         N    NE    E    SE    S    SW    W    NW
    dirmap = (64,  128,  1,   2,    4,   8,    16,  32)

    # fill / resolve flats / flow direction / accumulation
    grid.fill_depressions(data=dem, out_name='flooded_dem')
    grid.resolve_flats(data='flooded_dem', out_name='inflated_dem')
    grid.flowdir(data='inflated_dem', out_name='dir', dirmap=dirmap, as_crs=new_crs)
    grid.accumulation(data='dir', dirmap=dirmap, out_name='acc', apply_mask=False)

    # snap pour point to higher accumulation cells
    # (in theory, this shouldn't really be necesssary after burning in the
    # streams, but the DEM and streams are not exact matches)
    x, y = pourpoint_coord
    xy_snapped = grid.snap_to_mask(grid.acc > 50, [[x, y]], return_dist=False)
    x, y = xy_snapped[0][0], xy_snapped[0][1]

    # create catchment
    grid.catchment(data='dir', x=x, y=y, dirmap=dirmap, out_name='catch',
                   recursionlimit=15000, xytype='label', nodata_out=0)

    # Clip the bounding box to the catchment
    grid.clip_to('catch')

    # polygonize and return a list of polygon shapely objects
    return [geometry.shape(shape) for shape, value in grid.polygonize()]


def add_wsdrefine(prelim_wsd_table, ref_id, db=None):
    """
    Add local watersheds refined by cut method and dem method to the prelim watersheds
    table
    """
    # lower case ids only
    ref_id = ref_id.lower()

    if not db:
        db = fwa.util.connect()
    # watersheds refined by DEM are not always clean. Inserted based on an intersect
    # with hex polys to tidy them up
    sql = """INSERT INTO {prelim_wsd_table} ({ref_id}, source, geom)
             SELECT
              h.{ref_id},
              'DEM refined' as source,
             ST_Multi(ST_Union(h.geom)) as geom
            FROM public.wsdrefine_hexwsd h
            INNER JOIN public.wsdrefine_dem d
            ON h.{ref_id} = d.{ref_id}
            AND ST_Intersects(h.geom, d.geom)
            GROUP BY h.{ref_id}
          """.format(prelim_wsd_table=prelim_wsd_table,
                     ref_id=ref_id)
    db.execute(sql)
    sql = """INSERT INTO {prelim_wsd_table} ({ref_id}, source, geom)
             SELECT
              c.{ref_id},
              'CUT' as source,
             geom as geom
            FROM public.wsdrefine_cut c
          """.format(prelim_wsd_table=prelim_wsd_table,
                     ref_id=ref_id)
    db.execute(sql)


def add_ex_bc(point_table, ref_table, ref_id, out_table, db=None):
    """
    Insert contributing areas outside of BC into a watersheds table.

    Considers:
    1. For points inside of BC, contributing areas outside of BC
    2. Point locations outside of BC
    """
    # lower case ids only
    ref_id = ref_id.lower()
    if not db:
        db = fwa.util.connect()

    # first, process points already loaded to reference table (in BC)
    for fwa_point_event in db[ref_table]:
        ref_id_value = fwa_point_event[ref_id]

        # Do areas outside of BC contribute to the point (not including Alaska)
        db['public.wsdrefine_borderpts'].drop()
        db.execute(
            fwa.queries['upstream_border_crossing'],
            (str(fwa_point_event['wscode_ltree']),
             str(fwa_point_event['localcode_ltree']))
        )
        border_points = [r for r in db['public.wsdrefine_borderpts'].all()]
        if len(border_points):
            # If streams cross border to lower 48, use NHD WBD
            if border_points[0]['border'] == 'USA_49':
                log("processing border streams on 49")
                sql = """WITH RECURSIVE walkup (huc12, geom) AS
                    (
                        SELECT huc12, wsd.geom
                        FROM usgs.wbdhu12 wsd
                        INNER JOIN public.wsdrefine_borderpts pt
                        ON ST_Intersects(wsd.geom, pt.geom)
                        UNION ALL
                        SELECT b.huc12, b.geom
                        FROM usgs.wbdhu12 b,
                        walkup w
                        WHERE b.tohuc = w.huc12
                    )
                    INSERT INTO {out_table} ({ref_id}, source, geom)
                    SELECT
                      %s AS {ref_id},
                      'NHD HUC12' AS source,
                      ST_Union(geom)
                    FROM walkup
                """.format(out_table=out_table,
                           ref_id=ref_id)
                db.execute(sql, (ref_id_value))
            else:
                log("processing border streams not on 49")
                sql = """WITH RECURSIVE walkup (hybas_id, geom) AS
                    (
                        SELECT hybas_id, wsd.geom
                        FROM hydrosheds.hybas_lev12_v1c wsd
                        INNER JOIN public.wsdrefine_borderpts pt
                        ON ST_Intersects(wsd.geom, pt.geom)
                        UNION ALL
                        SELECT b.hybas_id, b.geom
                        FROM hydrosheds.hybas_lev12_v1c b,
                        walkup w
                        WHERE b.next_down = w.hybas_id
                    )
                    INSERT INTO {out_table} ({ref_id}, source, geom)
                    SELECT
                      %s AS {ref_id},
                      'hybas_na_lev12_v1c' AS source,
                      ST_Union(geom)
                    FROM walkup
                """.format(out_table=out_table,
                           ref_id=ref_id)
                db.execute(sql, (ref_id_value))


    # Attempt to process locations outside of BC (not aleady in the ref table)
    # use the API for this rather than the source data - it cuts off the watershed
    # at the point.
    sql = """SELECT
               a.{ref_id},
               ST_X(ST_Transform((ST_Dump(a.geom)).geom, 4326)) as x,
               ST_Y(ST_Transform((ST_Dump(a.geom)).geom, 4326)) as y
             FROM {point_table} a
             LEFT OUTER JOIN {ref_table} b
             ON a.{ref_id} = b.{ref_id}
             WHERE b.{ref_id} IS NULL
          """.format(ref_id=ref_id,
                     point_table=point_table,
                     ref_table=ref_table)
    locations = db.query(sql)
    for location in locations:
        log("Site {}: searching for non-BC streams".format(str(location[ref_id])))
        comid, measure, index_dist = epa_waters.index_point(
                                         location['x'],
                                         location['y'],
                                         150
                                     )
        log("    - found USA stream: comid: {com}, measure: {m}".format(
            com=comid, m=measure))
        wsd = epa_waters.delineate_watershed(
            location[ref_id],
            comid,
            measure
        )
        # Convert geojson to shapely object then insert into db
        if wsd:
            wsd = geometry.shape(geojson.loads(json.dumps(wsd)))
            sql = """
                INSERT INTO {out_table} ({ref_id}, geom, source)
                VALUES
                (
                 %s,
                 ST_Multi(ST_Transform(ST_GeomFromText(%s, 4326), 3005)),
                 'epa_waters'
                )
                """.format(out_table=out_table,
                           ref_id=ref_id)
            db.execute(sql, (location[ref_id], wsd.wkt))
        else:
            log('    - unsupported location, no watershed generated')

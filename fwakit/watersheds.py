import fwakit as fwa
from fwakit.util import log


def points_to_watersheds(in_table, in_id, out_table, dissolve=False,
                         include_bottom=False, db=None):
    """
    Create a table holding watersheds upstream of the referenced locations
    provided. Input table must include fields:
       - unique id (in_id),
       - wscode_ltree
       - localcode_ltree

    Note: this could perhaps be sped up significantly by using the pre-aggregated
    assessment watersheds as the first step of selection rather than building
    everything from scratch.
    """
    log('Creating watersheds upstream of points referenced to streams')
    if not db:
        db = fwa.util.connect()
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
          wsd.waterbody_key,
    -- note components of lakes and reservoirs
          CASE
            WHEN l.waterbody_key IS NOT NULL OR wb.waterbody_key IS NOT NULL
            THEN 'wb'
          END AS waterbody_ind,
          ST_Multi(ST_Force2D(wsd.geom)) as geom
        FROM {in_table} pt
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
    """.format(in_table=in_table, pk=in_id)
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
    SELECT {pk}, waterbody_key, geom
    FROM temp_prelim_wsds
    UNION
    SELECT lr.{pk}, lr.waterbody_key, ST_Multi(ST_Force2D(w.geom)) AS geom
    FROM
      (SELECT DISTINCT p.{pk}, p.waterbody_key
       FROM temp_prelim_wsds p
       WHERE waterbody_ind IS NOT NULL
      ) AS lr
    INNER JOIN whse_basemapping.fwa_watersheds_poly_sp w
    ON lr.waterbody_key = w.waterbody_key
    """.format(out_table=out_table, pk=in_id)
    db.execute(sql)
    db[out_table].create_index([in_id])
    db[out_table].create_index_geom()

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
        db['public.wsdrefine_agg'].drop()
        sql = """CREATE TABLE public.wsdrefine_agg
                 (LIKE {out_table})
              """.format(out_table=out_table)
        db.execute(sql)
        # get ids to iterate through
        sql = """SELECT DISTINCT {id}
                 FROM {in_table}
                 ORDER BY {id}
               """.format(id=in_id,
                          in_table=in_table)
        for record in db.query(sql).fetchall():
            site = record[in_id]
            log('Aggregating '+str(site))
            # run the clip/intersect
            sql = """INSERT INTO public.wsdrefine_agg
                     SELECT {ref_id},
                       CASE WHEN ST_Within(a.geom, b.geom) THEN a.geom
                            ELSE ST_Intersection(a.geom, b.geom)
                       END as geom
                    FROM whse_basemapping.fwa_watershed_groups_subdivided a
                    INNER JOIN public.wsdrefine_prelim b
                    ON ST_Intersects(a.geom, b.geom)
                    WHERE station = %s
              """.format(ref_id=in_id)
            db.execute(sql, (site,))
        # move the aggregated data over into the output table
        db[out_table].drop()
        db.execute("""CREATE TABLE {out_table} AS
                      SELECT * FROM public.wsdrefine_agg
                   """.format(out_table=out_table))
        # re-index the output
        db[out_table].create_index([in_id])
        db[out_table].create_index_geom()


def location_info(ref_table, ref_id, ref_id_value, db=None):
    """
    For provided table/id, return blue_line_key, measure, watershed codes,
    whether the location is on a waterbody and the measure at
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
                THEN riv.waterbody_key
                WHEN mmwb.waterbody_key IS NOT NULL
                 AND mmwb.feature_code = 'GA03950000'
                THEN mmwb.waterbody_key
                WHEN lk.waterbody_key IS NOT NULL
                THEN lk.waterbody_key
              END as waterbody_key,
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
            LEFT OUTER JOIN whse_basemapping.fwa_lakes_poly lk
            ON s.waterbody_key = lk.waterbody_key
            LEFT OUTER JOIN whse_basemapping.fwa_manmade_waterbodies_poly mmwb
            ON s.waterbody_key = mmwb.waterbody_key
            WHERE pts.{ref_id} = %s
        """.format(ref_table=ref_table, ref_id=ref_id)
    return db.query(sql, (ref_id_value)).fetchone()


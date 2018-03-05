import fwakit as fwa
from fwakit.util import log


def points_to_watersheds(in_table, in_id, out_table, dissolve=False, db=None):
    """
    Create a table holding watersheds upstream of the referenced locations
    provided. Input table must include fields:
       - unique id (in_id),
       - wscode_ltree
       - localcode_ltree
    """
    log('Creating watersheds upstream of provided points')
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
                THEN riv.waterbody_key
                WHEN mmwb.waterbody_key IS NOT NULL
                 AND mmwb.feature_code = 'GA03950000'
                THEN mmwb.waterbody_key
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
            LEFT OUTER JOIN whse_basemapping.fwa_manmade_waterbodies_poly mmwb
            ON s.waterbody_key = mmwb.waterbody_key
            WHERE pts.{ref_id} = %s
        """.format(ref_table=ref_table, ref_id=ref_id)
    return db.query(sql, (ref_id_value)).fetchone()


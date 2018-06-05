-- find the watershed in which the point lies and insert as is into output table
WITH stn_point AS (
  SELECT
    e.$ref_id,
    e.blue_line_key,
    e.downstream_route_measure,
    e.linear_feature_id,
    e.wscode_ltree,
    e.localcode_ltree,
    ST_LineInterpolatePoint(
      ST_LineMerge(s.geom),
        ROUND(
          CAST(
            (e.downstream_route_measure - s.downstream_route_measure) /
             s.length_metre AS NUMERIC),
          5)
        ) AS geom
  FROM $ref_table e
  INNER JOIN whse_basemapping.fwa_stream_networks_sp s
  ON e.linear_feature_id = s.linear_feature_id
  WHERE e.$ref_id = %s
)

INSERT INTO $out_table ($ref_id, source, geom)

SELECT
  pt.$ref_id,
  'non-refined' as source,
  ST_Multi(wsd.geom) as geom
 FROM whse_basemapping.fwa_watersheds_poly_sp wsd
 INNER JOIN stn_point pt
 ON ST_DWithin(wsd.geom, pt.geom, 5)

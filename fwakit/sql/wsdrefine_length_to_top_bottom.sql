-- Find the measure at the max extent of the watershed polygon in which the
-- point lies. This is a simple query against the watershed codes for the most
-- part, but there are cases where the watershed polygon does not terminate
-- at the same spot where the local code changes (eg edges of lakes). The
-- spatial query at the end ensures that only stream within the watersehd in
-- question is returned.

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
),

wsd AS
(SELECT
  wsd.watershed_feature_id,
  wsd.geom
 FROM whse_basemapping.fwa_watersheds_poly_sp wsd
 INNER JOIN stn_point pt
 ON ST_DWithin(wsd.geom, pt.geom, 5)
),

length_to_top AS
(SELECT
  (str.downstream_route_measure + str.length_metre) - stn.downstream_route_measure AS measure
FROM whse_basemapping.fwa_stream_networks_sp str
INNER JOIN stn_point stn
  ON str.blue_line_key = stn.blue_line_key
  AND str.localcode_ltree = stn.localcode_ltree
INNER JOIN wsd ON ST_CoveredBy(str.geom, wsd.geom)
ORDER BY str.downstream_route_measure desc
LIMIT 1),

length_to_bottom AS
(SELECT
  stn.downstream_route_measure - str.downstream_route_measure AS measure
FROM whse_basemapping.fwa_stream_networks_sp str
INNER JOIN stn_point stn
  ON str.blue_line_key = stn.blue_line_key
  AND str.localcode_ltree = stn.localcode_ltree
INNER JOIN wsd ON ST_CoveredBy(str.geom, wsd.geom)
ORDER BY str.downstream_route_measure asc
LIMIT 1)

SELECT t.measure, b.measure
FROM length_to_top t, length_to_bottom b
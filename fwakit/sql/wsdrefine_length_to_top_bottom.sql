-- Find the measure at the max extent of the watershed polygon in which a point event
-- lies. This is a simple query against the watershed codes for the most
-- part, but there are cases where the watershed polygon does not terminate
-- at the same spot where the local code changes (eg edges of lakes). The
-- spatial query at the end ensures that only stream within the watershed in
-- question is returned.

WITH ref_point AS (
  SELECT
    :blue_line_key as blue_line_key,
    :downstream_route_measure as downstream_route_measure,
    :linear_feature_id as linear_feature_id,
    text2ltree(:wscode_ltree) as wscode_ltree,
    text2ltree(:localcode_ltree) as localcode_ltree,
    ST_LineInterpolatePoint(
      ST_LineMerge(s.geom),
        ROUND(
          CAST(
            (:downstream_route_measure - s.downstream_route_measure) /
             s.length_metre AS NUMERIC),
          5)
        ) AS geom
  FROM whse_basemapping.fwa_stream_networks_sp s
  WHERE linear_feature_id = :linear_feature_id
),

wsd AS
(SELECT
  ST_Union(wsd.geom) as geom
 FROM whse_basemapping.fwa_watersheds_poly_sp wsd
 INNER JOIN ref_point pt
 ON ST_DWithin(wsd.geom, pt.geom, 5)
),

length_to_top AS
(SELECT
  (str.downstream_route_measure + str.length_metre) - refpt.downstream_route_measure AS measure
FROM whse_basemapping.fwa_stream_networks_sp str
INNER JOIN ref_point refpt
  ON str.blue_line_key = refpt.blue_line_key
  AND str.wscode_ltree = refpt.wscode_ltree
INNER JOIN wsd ON ST_CoveredBy(str.geom, wsd.geom)
ORDER BY str.downstream_route_measure desc
LIMIT 1),

length_to_bottom AS
(SELECT
  refpt.downstream_route_measure - str.downstream_route_measure AS measure
FROM whse_basemapping.fwa_stream_networks_sp str
INNER JOIN ref_point refpt
  ON str.blue_line_key = refpt.blue_line_key
  AND str.wscode_ltree = refpt.wscode_ltree
INNER JOIN wsd ON ST_CoveredBy(str.geom, wsd.geom)
ORDER BY str.downstream_route_measure asc
LIMIT 1)

SELECT t.measure, b.measure
FROM length_to_top t, length_to_bottom b
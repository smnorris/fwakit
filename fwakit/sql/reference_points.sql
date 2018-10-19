-- Create event table from provided points
-- note that this will return up to 100 results per point,
-- every stream within the tolerance is returned

-- this would be much better as a postgres function where we are not injecting
-- sql - but how do we pass the point table and id to the CTE?

DROP TABLE IF EXISTS $out_table;

CREATE TABLE $out_table AS

WITH candidates AS
 ( SELECT
    pt.$point_id,
    nn.linear_feature_id,
    nn.wscode_ltree,
    nn.localcode_ltree,
    nn.fwa_watershed_code,
    nn.local_watershed_code,
    nn.blue_line_key,
    nn.length_metre,
    nn.downstream_route_measure,
    nn.distance_to_stream,
    nn.watershed_group_code,
    ST_LineMerge(nn.geom) AS geom
  FROM $point_table as pt
  CROSS JOIN LATERAL
  (SELECT
     str.linear_feature_id,
     str.wscode_ltree,
     str.localcode_ltree,
     str.fwa_watershed_code,
     str.local_watershed_code,
     str.blue_line_key,
     str.length_metre,
     str.downstream_route_measure,
     str.watershed_group_code,
     str.geom,
     ST_Distance(str.geom, pt.geom) as distance_to_stream
    FROM whse_basemapping.fwa_stream_networks_sp AS str
    WHERE str.localcode_ltree IS NOT NULL
    AND NOT str.wscode_ltree <@ '999'
    ORDER BY str.geom <-> pt.geom
    LIMIT 100) as nn
  WHERE nn.distance_to_stream < %s
),

bluelines AS
(SELECT DISTINCT ON ($point_id, blue_line_key)
  $point_id,
  blue_line_key,
  distance_to_stream
FROM candidates
ORDER BY $point_id, blue_line_key, distance_to_stream
)

SELECT
  bluelines.$point_id,
  candidates.linear_feature_id,
  candidates.wscode_ltree,
  candidates.localcode_ltree,
  candidates.fwa_watershed_code,
  candidates.local_watershed_code,
  bluelines.blue_line_key,
  (ST_LineLocatePoint(candidates.geom,
                       ST_ClosestPoint(candidates.geom, pts.geom))
     * candidates.length_metre) + candidates.downstream_route_measure
    AS downstream_route_measure,
  candidates.distance_to_stream,
  candidates.watershed_group_code
FROM bluelines
INNER JOIN candidates ON bluelines.$point_id = candidates.$point_id
AND bluelines.blue_line_key = candidates.blue_line_key
AND bluelines.distance_to_stream = candidates.distance_to_stream
INNER JOIN $point_table pts ON bluelines.$point_id = pts.$point_id
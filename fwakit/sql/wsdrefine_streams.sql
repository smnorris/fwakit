-- extract the stream on which the point lies, pulling only the geometry with
-- a measure greater than the measure of the location
WITH stn_point AS (
  SELECT
    e.station,
    e.blue_line_key,
    e.downstream_route_measure,
    e.linear_feature_id,
    e.wscode_ltree,
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

-- get stream at the site location, returning only the gemetry upstream of site
stream_at_pt AS
(SELECT
  s.linear_feature_id,
  s.blue_line_key,
  s.downstream_route_measure,
  s.wscode_ltree,
  s.localcode_ltree,
  ST_LineSubstring((ST_Dump(s.geom)).geom,
                   ((e.downstream_route_measure - s.downstream_route_measure) / s.length_metre),
                   1) AS geom
FROM stn_point e
INNER JOIN whse_basemapping.fwa_stream_networks_sp s
ON e.linear_feature_id = s.linear_feature_id),

-- find all streams upstream of site
stream_upstream AS
(
  SELECT
    b.linear_feature_id,
    b.blue_line_key,
    b.wscode_ltree,
    b.geom
  FROM stream_at_pt a
  LEFT OUTER JOIN whse_basemapping.fwa_stream_networks_sp b
  ON b.wscode_ltree <@ a.wscode_ltree
  AND b.localcode_ltree != a.localcode_ltree
  AND b.linear_feature_id != a.linear_feature_id
  AND
    CASE
       WHEN
          a.wscode_ltree = a.localcode_ltree AND
          (
              (b.blue_line_key <> a.blue_line_key OR
               b.downstream_route_measure > a.downstream_route_measure + .001)
          )
       THEN TRUE
       WHEN
          a.wscode_ltree != a.localcode_ltree AND
          (
              (b.blue_line_key = a.blue_line_key AND
               b.downstream_route_measure > a.downstream_route_measure + .001)
              OR
              (b.wscode_ltree > a.localcode_ltree AND
               NOT b.wscode_ltree <@ a.localcode_ltree)
              OR
              (b.wscode_ltree = a.wscode_ltree
               AND b.localcode_ltree >= a.localcode_ltree)
          )
        THEN TRUE
    END
)

-- return only streams with equivalent watershed code
CREATE TABLE wsdrefine_streams AS
SELECT linear_feature_id, blue_line_key, geom
FROM stream_at_pt
UNION ALL
SELECT u.linear_feature_id, u.blue_line_key, u.geom
FROM stream_upstream u
INNER JOIN stn_point s ON u.wscode_ltree = s.wscode_ltree

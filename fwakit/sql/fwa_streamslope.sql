-- fwa_streamslope(blue_line_key, downstream_route_measure)

-- Return slope of the stream at the location provided by blue_line_key and
-- downstream_route_measure


CREATE OR REPLACE FUNCTION fwa_streamprofile(
    blkey integer
)

RETURNS double precision AS $$

WITH total_len AS
(SELECT
    blue_line_key,
    downstream_route_measure + length_metre AS blue_line_length
 FROM whse_basemapping.fwa_stream_networks_sp
 WHERE blue_line_key = blkey
 -- do not include lines outside of BC
 AND edge_type != 6010
 ORDER BY downstream_route_measure DESC
 LIMIT 1
),

line AS
(SELECT
  s.blue_line_key,
  s.linear_feature_id,
  length_metre,
  total_len.blue_line_length,
  downstream_route_measure,
  (ST_Dump(geom)).geom AS geom
FROM whse_basemapping.fwa_stream_networks_sp s
INNER JOIN total_len ON s.blue_line_key = total_len.blue_line_key
AND downstream_route_measure < (measure + .001)
WHERE s.blue_line_key = s.watershed_key
ORDER BY downstream_route_measure DESC
LIMIT 1),

vertices AS
(
  SELECT
    blue_line_key,
    linear_feature_id,
    generate_series(1, ST_NPoints(geom)),
    ((ST_LineLocatePoint(geom, ST_PointN(geom, generate_series(1, ST_NPoints(geom)))) * length_metre) + downstream_route_measure) / blue_line_length  AS pct,
    ST_Z(ST_PointN(geom, generate_series(1, ST_NPoints(geom)))) AS elevation,
    downstream_route_measure,
    blue_line_length
  FROM line
),

-- create edges between the vertices, as from and to percentages and elevations
prelim_edges AS
(SELECT
   ROW_NUMBER() OVER(ORDER BY pct) AS id,
   pct AS from_pct,
   lead(pct) OVER(ORDER BY pct) AS to_pct,
   elevation AS from_elevation,
   lead(elevation) OVER(ORDER BY pct) AS to_elevation,
   blue_line_length
FROM vertices
),

-- calculate length of each edge, slope and clean up the end
edges AS
(SELECT
  row_number() over() as id,
  from_pct,
  to_pct,
  blue_line_length * (to_pct - from_pct) AS length_metres,
  from_elevation,
  to_elevation,
  (to_elevation - from_elevation) / (blue_line_length * (to_pct - from_pct)) AS slope
FROM prelim_edges
WHERE round(from_pct::numeric, 5) <> 1
-- make sure the data is good
AND to_elevation IS NOT null
-- don't duplicate points, we don't want to divide by zero when calculating slope
AND to_pct != from_pct
-- double check things are in order
ORDER BY from_pct)

-- get slope at a point by getting all slopes less than that measure,
-- and only returning the first result
SELECT
  slope
FROM edges
WHERE from_pct <
  (measure /
    (SELECT blue_line_length
     FROM total_len)
  )
ORDER BY from_pct desc
LIMIT 1;

$$
language 'sql' immutable strict parallel safe;
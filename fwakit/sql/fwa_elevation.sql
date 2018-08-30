-- Return the elevation of a stream at a point
-- Point is defined by blue_line_key and downstream_route_measure

CREATE OR REPLACE FUNCTION fwa_elevation(
    blkey integer,
    measure double precision
)

RETURNS numeric AS $$

-- Extract the stream segment with the matching blue_line_key and
-- downstream_route_measure, and:
--   - convert the source multiline to single-part line
--   - derive fraction of segment linestring length at which measure occurs

WITH segment AS
(SELECT
  ROUND(((measure - downstream_route_measure) / length_metre)::numeric, 5) AS pct_seg,
  ST_LineMerge(geom) as geom
FROM whse_basemapping.fwa_stream_networks_sp
WHERE blue_line_key = blkey
--AND blue_line_key = watershed_key
AND downstream_route_measure <= measure
ORDER BY downstream_route_measure desc
LIMIT 1)

SELECT
  ROUND(
    ST_Z(
      ST_LineInterpolatePoint(
        geom,
        pct_seg
      )
    )::numeric,
    2) as elevation
FROM segment

$$
language 'sql' immutable strict parallel safe;
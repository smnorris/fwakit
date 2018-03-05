-- wsd_hex.sql
-- Create a hex grid covering the watershed in which a site falls

-- Generate a point from the measure of the site location on the stream
WITH stn_point AS (
  SELECT
    e.$ref_id,
    e.blue_line_key,
    e.downstream_route_measure,
    s.waterbody_key,
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
  WHERE $ref_id = %s
),

-- find the watershed in which the point falls
stn_wsd AS (
  SELECT w.watershed_feature_id, w.geom
  FROM stn_point p
  INNER JOIN whse_basemapping.fwa_watersheds_poly_sp w
  ON ST_Intersects(p.geom, w.geom)
),

-- generate a hex grid (with 25m sides) covering the entire watershed polygon
hex_grid AS (
  SELECT CDB_HexagonGrid(ST_Buffer(geom, 25), 25) as geom
  FROM stn_wsd
)

-- cut the hex grid by the watershed boundary and write to a new table
CREATE TABLE wsdrefine_hex_wsd AS
(SELECT
  CASE
    WHEN ST_Within(a.geom, b.geom) THEN a.geom
    ELSE ST_Intersection(a.geom, b.geom)
  END as geom
 FROM hex_grid a
INNER JOIN stn_wsd b ON ST_Intersects(a.geom, b.geom))

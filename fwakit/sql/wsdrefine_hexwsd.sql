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
  SELECT p.$ref_id, w.watershed_feature_id, w.geom
  FROM stn_point p
  INNER JOIN whse_basemapping.fwa_watersheds_poly_sp w
  ON ST_Intersects(p.geom, w.geom)
),

-- generate a hex grid (with 25m sides) covering the entire watershed polygon
hex_grid AS (
  SELECT ST_Force2D(CDB_HexagonGrid(ST_Buffer(geom, 25), 25)) as geom
  FROM stn_wsd
)

-- cut the hex grid by the watershed boundary and write to output table
INSERT INTO public.wsdrefine_hexwsd ($ref_id, geom)
SELECT
  b.$ref_id,
  CASE
    WHEN ST_Within(a.geom, b.geom) THEN ST_Multi(a.geom)
    ELSE ST_Multi(ST_Force2D(ST_Intersection(a.geom, b.geom)))
  END as geom
 FROM hex_grid a
INNER JOIN stn_wsd b ON ST_Intersects(a.geom, b.geom)

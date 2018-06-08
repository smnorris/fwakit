-- Return linear_feature_id of streams that likely leave BC
-- Includes streams bordering Yukon, NWT, Alberta, Montana, Idaho, Washington

-- NOTES:
-- - this does not currently include Alaska
-- - we do not use the official border as it does not precisely follow 49/60/120
--   (but the FWA edges generally do)
-- - presumes no streams span the AB/BC continental divide portion of border
-- - it could be valuable to manually QA the results and generate a lookup of all
--   BC streams that exit the province rather than approximating in this way

CREATE TABLE public.wsdrefine_borderpts AS

with borders_approx AS
(SELECT
  'USA_49' as border,
    ST_Transform(
      ST_MakeLine(
        ST_SetSRID(ST_MakePoint(x, y), 4326)
      ),
    3005)
   AS geom
FROM (SELECT
        generate_series(-123.3, -114.06, .01) AS x,
        49.0005 AS y) AS segments

UNION ALL

SELECT
  'YTNWT_60' as border,
    ST_Transform(
      ST_MakeLine(
        ST_SetSRID(ST_MakePoint(x, y), 4326)
      ),
    3005)
   AS geom
FROM (SELECT
        generate_series(-139.05, -120.00, .01) AS x,
        59.9995 AS y) AS segments

UNION ALL

SELECT
  'AB_120' as border,
    ST_Transform(
      ST_MakeLine(
        ST_SetSRID(ST_MakePoint(x, y), 4326)
      ),
    3005)
   AS geom
FROM (SELECT
        -120.0005 AS x,
        generate_series(60, 53.79914, -.01) AS y) AS segments),

intersections AS
(SELECT
  b.border,
  s.linear_feature_id,
  ST_ClosestPoint(ST_Translate(b.geom, 0, -75),
    ST_Intersection(s.geom, b.geom)
    ) as geom
FROM whse_basemapping.fwa_stream_networks_sp s
INNER JOIN borders_approx  b
ON ST_Intersects(s.geom, b.geom)
WHERE FWA_UpstreamWSC(%s::ltree, %s::ltree, s.wscode_ltree, s.localcode_ltree))


SELECT
 border,
 linear_feature_id,
 ST_X(ST_Transform(geom, 4326)) as x,
 ST_Y(ST_Transform(geom, 4326)) as y,
 geom
FROM intersections



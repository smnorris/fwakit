-- For sites that are on double line rivers or canals, extract the watersheds
-- on the waterbody and on the banks of the waterbody adjacent to the site.
-- Then cut these polys from the site location to the closest point on the
-- opposite edge of the bank/adjacent watershed poly

-- Generate a point from the measure of the site location on the stream
WITH stn_point AS
(SELECT
  e.$ref_id,
  e.blue_line_key,
  e.downstream_route_measure,
  s.waterbody_key,
  ST_LineInterpolatePoint(ST_LineMerge(s.geom), ROUND(CAST((e.downstream_route_measure - s.downstream_route_measure) / s.length_metre AS NUMERIC), 5)) AS geom
  FROM $ref_table e
 INNER JOIN whse_basemapping.fwa_stream_networks_sp s
ON e.linear_feature_id = s.linear_feature_id
WHERE $ref_id = %s),

-- Find watershed polys that compose the river on which the point lies.
-- This is not a simple case of extracting watersheds with the equivalent
-- waterbody key, waterbodies may terminate near a site, we may have to
-- include several watershed polygons.
-- Therefore, here we will first select watersheds with a matching wb key
-- (within 100m) and then in the next WITH CTE, widen the selection to
-- any watersheds with that touch the waterbody in which the point lies.
wsds_river_prelim AS
(SELECT
  wsd.watershed_feature_id,
  wsd.waterbody_key,
  wsd.geom
 FROM whse_basemapping.fwa_watersheds_poly_sp wsd
 INNER JOIN stn_point pt
  ON (wsd.waterbody_key = pt.waterbody_key
     AND ST_DWithin(wsd.geom, pt.geom, 100))
     ),

-- add intersectiong waterbodies if present, combining with results from above
wsds_river AS
(SELECT DISTINCT watershed_feature_id, waterbody_key, geom
FROM (
(SELECT wsd.watershed_feature_id, wsd.waterbody_key, wsd.geom
FROM whse_basemapping.fwa_watersheds_poly_sp wsd
INNER JOIN wsds_river_prelim p ON ST_Intersects(wsd.geom, p.geom)
WHERE wsd.watershed_feature_id != p.watershed_feature_id
AND wsd.waterbody_key != 0
UNION ALL
SELECT * FROM wsds_river_prelim)
) as foo
) ,


-- find the watershed polygons that are on the banks of wsds_river, returns
-- all watersheds that share an edge with the river (or lake) polys
wsds_adjacent AS
(SELECT
    r.watershed_feature_id as riv_id,
  wsd.watershed_feature_id,
  wsd.geom,
  ST_Distance(s.geom, wsd.geom) as dist_to_site
FROM whse_basemapping.fwa_watersheds_poly_sp wsd
INNER JOIN wsds_river r
ON (r.geom && wsd.geom AND ST_Relate(r.geom, wsd.geom, '****1****'))
INNER JOIN stn_point s ON ST_DWithin(s.geom, r.geom, 5)
LEFT OUTER JOIN whse_basemapping.fwa_lakes_poly lk
ON wsd.waterbody_key = lk.waterbody_key
LEFT OUTER JOIN whse_basemapping.fwa_rivers_poly riv
ON wsd.waterbody_key = riv.waterbody_key
LEFT OUTER JOIN whse_basemapping.fwa_manmade_waterbodies_poly mm
ON wsd.waterbody_key = mm.waterbody_key
WHERE lk.waterbody_key IS NULL AND riv.waterbody_key IS NULL AND mm.waterbody_key IS NULL
AND r.watershed_feature_id != wsd.watershed_feature_id
AND wsd.watershed_feature_id NOT IN (SELECT watershed_feature_id FROM wsds_river)),

-- From wsds_adjacent, find just the nearest wsd poly to the point (on each
-- bank) - there should always be just two results
wsds_adjacent_nearest AS
(SELECT DISTINCT ON (riv_id) riv_id, watershed_feature_id, dist_to_site, geom
FROM wsds_adjacent
ORDER BY riv_id, dist_to_site),

-- Extract the exterior ring from wsds_adjacent_nearest and retain only the
-- portion that doesn't intersect with the river polys - the outside edges
edges AS (SELECT
   w_adj.watershed_feature_id,
   ST_Difference(ST_ExteriorRing((ST_Dump(w_adj.geom)).geom), w_riv.geom ) as geom
FROM wsds_adjacent_nearest w_adj,
(SELECT ST_Union(geom) as geom FROM wsds_river) as w_riv),

-- Find the closest points along edges to the site, this is where we will
-- cut the watersheds
cut_line_ends AS (SELECT
  row_number() over() as id,
  ST_ClosestPoint(edges.geom, stn.geom) as geom
FROM edges, stn_point stn),

-- Build a line between the points and the site itself, creating the cutting
-- blade. Make sure the points are ordered correctly when building the line.
blade AS (
SELECT 1 as id, ST_MakeLine(geom) geom FROM
(SELECT
  CASE WHEN id = 2 THEN 3
  ELSE id
  END AS id,
  geom
FROM cut_line_ends
UNION ALL
SELECT 2 AS id, geom FROM stn_point
ORDER BY id) as orderedpts),

--- Aggregate the watersheds extracted above (river and nearest adjacent) into
-- a single poly for cutting. Insert other nearby waterbodies in case we are
-- missing part of the river when sharp angles get involved
to_split AS
(SELECT (ST_Dump(ST_Union(geom))).geom AS geom FROM
(SELECT geom FROM wsds_adjacent_nearest
 UNION ALL
 SELECT geom FROM wsds_river
 UNION ALL
 SELECT wsd.geom FROM whse_basemapping.fwa_watersheds_poly_sp wsd
 INNER JOIN stn_point pt
 ON ST_DWithin(wsd.geom, pt.geom, 100)
 WHERE wsd.waterbody_key != 0) AS bar)

-- Cut the aggregated watershed poly and insert the results into a temp table
-- for adding to the prelim watersheds
INSERT INTO public.wsdrefine_prelim ($ref_id, source, geom)

SELECT
  stream.$ref_id,
  'wb cut' AS source,
  ST_Multi(ST_Force2d(baz.geom)) AS geom
FROM
(SELECT
 (ST_Dump(ST_Split(ST_Snap(w.geom, b.geom, .001), b.geom))).geom
FROM to_split w, blade b) AS baz
INNER JOIN
(SELECT p.$ref_id, str.geom FROM whse_basemapping.fwa_stream_networks_sp str
 INNER JOIN stn_point p
 ON str.blue_line_key = p.blue_line_key
 AND str.downstream_route_measure > p.downstream_route_measure
 ORDER BY str. downstream_route_measure asc
 LIMIT 1) stream
 ON st_intersects(baz.geom, stream.geom)
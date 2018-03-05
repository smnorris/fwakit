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

-- find watershed polys that compose the river on which the point lies
-- Note that sometimes the waterbody keys may not match the wb key generated
-- in query above - hence the OR ST_DWithin, to make sure all river polys at
-- the location are captured (See wb key 328991070 for an example, on
-- the Thomposon at Kamloops)
wsds_river AS
(SELECT
  wsd.watershed_feature_id,
  wsd.waterbody_key,
  wsd.geom
 FROM whse_basemapping.fwa_watersheds_poly_sp wsd
 INNER JOIN stn_point pt
  ON (wsd.waterbody_key = pt.waterbody_key
     AND ST_DWithin(wsd.geom, pt.geom, 100))
     OR ST_DWithin(wsd.geom, pt.geom, 5)),

-- find the watershed polygons that are on the banks of wsds_river, returns
-- all watersheds that share an edge with the river polys
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
edges AS (SELECT w_adj.watershed_feature_id,
   w_riv.watershed_feature_id as w_riv_id,
   ST_Difference(ST_ExteriorRing((ST_Dump(w_adj.geom)).geom), w_riv.geom ) as geom
FROM wsds_adjacent_nearest w_adj
INNER JOIN wsds_river w_riv ON w_adj.riv_id = w_riv.watershed_feature_id),

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

-- Aggregate the watersheds extracted above (river and nearest adjacent) into
-- a single poly for cutting
to_split AS
(SELECT st_union(geom) AS geom FROM
(SELECT geom FROM wsds_adjacent_nearest
 UNION ALL
 SELECT geom FROM wsds_river) as bar)

-- Cut the aggregated watershed poly and insert the results into a temp table
-- for adding to the prelim watersheds
INSERT INTO wsdrefine_river_wsd_cut (geom)

SELECT baz.geom as geom
FROM
(SELECT
 (ST_Dump(ST_Split(ST_Snap(w.geom, b.geom, .001), b.geom))).geom
FROM to_split w, blade b) AS baz
INNER JOIN
(SELECT str.geom FROM whse_basemapping.fwa_stream_networks_sp str
 INNER JOIN stn_point p
 ON str.blue_line_key = p.blue_line_key
 AND str.downstream_route_measure > p.downstream_route_measure
 ORDER BY str. downstream_route_measure asc
 LIMIT 1) stream
 ON st_intersects(baz.geom, stream.geom)

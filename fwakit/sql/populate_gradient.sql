-- Populate the gradient column for specified watershed group
-- Dumping the geoms is necessary because they are loaded as MultiLinestrings,
-- which are not accepted by ST_PointN

WITH segment AS
(SELECT
  linear_feature_id,
  (ST_Dump(geom)).geom as geom
FROM whse_basemapping.fwa_stream_networks_sp
WHERE watershed_group_code = %s)

UPDATE whse_basemapping.fwa_stream_networks_sp as streams
SET gradient = round((((ST_Z(ST_PointN(seg.geom, -1)) - ST_Z(ST_PointN(seg.geom, 1))) / ST_Length(seg.geom))::numeric), 4)
FROM segment as seg
WHERE streams.linear_feature_id = seg.linear_feature_id;

-- The source FWA database holds waterbodies in four different tables.
-- We could combine them on demand using UNION, but for faster lookups (and shorter
-- queries), we can just create a simple waterbody table

CREATE TABLE whse_basemapping.fwa_waterbodies AS
  SELECT DISTINCT waterbody_key, waterbody_type
  FROM whse_basemapping.fwa_lakes_poly
UNION ALL
  SELECT DISTINCT waterbody_key, waterbody_type
  FROM whse_basemapping.fwa_wetlands_poly
UNION ALL
  SELECT DISTINCT waterbody_key, waterbody_type
  FROM whse_basemapping.fwa_rivers_poly
UNION ALL
  SELECT DISTINCT waterbody_key, waterbody_type
  FROM whse_basemapping.fwa_manmade_waterbodies_poly;

CREATE INDEX ON whse_basemapping.fwa_waterbodies (waterbody_key);
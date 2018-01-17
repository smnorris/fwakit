-- Create a subdivided watershed group table for *much* faster point in poly
-- queries and other spatial join operations.

DROP TABLE IF EXISTS whse_basemapping.fwa_watershed_groups_subdivided;

CREATE TABLE whse_basemapping.fwa_watershed_groups_subdivided
(fwa_watershed_groups_subdivided_id SERIAL PRIMARY KEY,
watershed_group_code text,
geom geometry);

INSERT INTO whse_basemapping.fwa_watershed_groups_subdivided
(watershed_group_code, geom)
SELECT
  watershed_group_code,
  ST_Subdivide(ST_Force2D(geom)) as geom
FROM whse_basemapping.fwa_watershed_groups_poly;

CREATE INDEX fwa_wsg_subdgeomidx
ON whse_basemapping.fwa_watershed_groups_subdivided USING gist (geom);
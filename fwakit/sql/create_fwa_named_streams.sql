-- create a table of named streams (for labelling purposes)
-- filter out lakes by joining to lakes/manmade wb tables
-- (edge type doesn't work because we want to retain rivers, wetlands)

DROP TABLE IF EXISTS whse_basemapping.fwa_named_streams;

CREATE TABLE whse_basemapping.fwa_named_streams
(fwa_stream_networks_label_id SERIAL PRIMARY KEY,
 gnis_name TEXT,
 watershed_group_code TEXT,
 geom GEOMETRY);

INSERT INTO whse_basemapping.fwa_named_streams
  (gnis_name, watershed_group_code, geom)
SELECT
  str.gnis_name,
  str.watershed_group_code,
  ST_Simplify(ST_Union(str.geom), 25) AS geom
  FROM whse_basemapping.fwa_stream_networks_sp str
  LEFT OUTER JOIN whse_basemapping.fwa_lakes_poly lk
  ON str.waterbody_key = lk.waterbody_key
  LEFT OUTER JOIN whse_basemapping.fwa_manmade_waterbodies_poly mmwb
  ON str.waterbody_key = mmwb.waterbody_key
  WHERE gnis_name IS NOT NULL
  AND lk.waterbody_key IS NULL
  AND mmwb.waterbody_key IS NULL
  GROUP BY str.gnis_name, str.watershed_group_code;
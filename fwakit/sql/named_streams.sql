drop table if exists whse_basemapping.fwa_stream_networks_label;

create table whse_basemapping.fwa_stream_networks_label
(fwa_stream_networks_label_id SERIAL PRIMARY KEY,
 gnis_name text,
 watershed_group_code text,
 geom geometry);


INSERT INTO whse_basemapping.fwa_stream_networks_label
(gnis_name, watershed_group_code, geom)
SELECT
  gnis_name,
  watershed_group_code,
  st_simplify(st_union(geom), 25) as geom
  FROM whse_basemapping.fwa_stream_networks_sp
  WHERE gnis_name is not null
  --AND edge_type in (1000, 1100, 1800, 1850, 2000, 2100)
  GROUP BY gnis_name, watershed_group_code;
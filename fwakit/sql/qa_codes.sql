SELECT
  linear_feature_id,
  blue_line_key,
  wscode_ltree,
  localcode_ltree
FROM whse_basemapping.fwa_stream_networks_sp
WHERE nlevel(wscode_ltree) != nlevel(localcode_ltree) AND nlevel(wscode_ltree) != nlevel(localcode_ltree) - 1;
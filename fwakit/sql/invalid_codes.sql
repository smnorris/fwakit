CREATE TABLE IF NOT EXISTS $OutTable AS
SELECT
  linear_feature_id,
  blue_line_key,
  wscode_ltree,
  localcode_ltree
FROM $InTable
WHERE nlevel(wscode_ltree) != nlevel(localcode_ltree)
AND nlevel(wscode_ltree) != nlevel(localcode_ltree) - 1
ORDER BY linear_feature_id;
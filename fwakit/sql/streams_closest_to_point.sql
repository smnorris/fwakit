-- select distinct on in case there are multiple results for the same stream
SELECT * FROM (
SELECT DISTINCT ON (fwa_watershed_code)
  fwa_watershed_code, distance_to_stream
FROM
(
  SELECT
    nn.fwa_watershed_code,
    nn.distance_to_stream
  FROM $inputPointTable pt
  CROSS JOIN LATERAL
  (SELECT
     str.fwa_watershed_code,
     ST_Distance(str.geom, pt.geom) as distance_to_stream
    FROM whse_basemapping.fwa_stream_networks_sp AS str
    WHERE str.blue_line_key = str.watershed_key
    AND str.fwa_watershed_code NOT LIKE '999-999%%'
    ORDER BY str.geom <-> pt.geom
    LIMIT 100) as nn
  WHERE $inputPointId = %s
  AND nn.distance_to_stream < %s
  ORDER BY distance_to_stream
) AS foo
ORDER BY fwa_watershed_code, distance_to_stream
) as bar
ORDER BY distance_to_stream
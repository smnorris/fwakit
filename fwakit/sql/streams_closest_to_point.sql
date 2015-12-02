-- Find streams closest to a given point
-- http://boundlessgeo.com/2011/09/indexed-nearest-neighbour-search-in-postgis/
-- http://suite.opengeo.org/4.1/dataadmin/pgBasics/indx_knn.html
WITH pt AS
(SELECT geom
   FROM $inputPointTable
   WHERE id = %s),

closest_candidates AS (
SELECT
  stream.fwa_watershed_code,
  stream.geom
FROM
  whse_basemapping.fwa_stream_networks_sp stream
-- do not consider side channels or isolated streams
WHERE blue_line_key = watershed_key
  AND fwa_watershed_code NOT LIKE '999-999%%'
-- can't use the cte, index doesn't kick in
ORDER BY stream.geom <-> (SELECT geom
                           FROM $inputPointTable
                          WHERE id = %s)
LIMIT 100)

-- now pull just streams within given tolerance from the candidates
-- note that this query may need to be simplified if running postgresql 9.5+
-- group the streams by watershed code, we don't want multiple results for a
-- single stream
SELECT
  fwa_watershed_code,
  min(distance_to_stream) as distance_to_stream
FROM
  (SELECT
    cc.fwa_watershed_code,
    ST_Distance(cc.geom, pt.geom) AS distance_to_stream
   FROM closest_candidates cc, pt
  WHERE ST_Distance(cc.geom, pt.geom) < %s) as foo
GROUP BY fwa_watershed_code
ORDER BY distance_to_stream
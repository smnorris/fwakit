-- fwa_lengthdownstream_recursive()

-- Return length of stream downstream of a point, given blue_line_key and
-- downstream_route_measure.
-- Note that this returns side channels/distributaries as well as main-stems.

-- Downstream query is recursive SQL adapted from:
-- http://blog.cleverelephant.ca/2010/07/network-walking-in-postgis.html

-- Due to its recursive nature, this function will only work when used with
-- the stream network and is provided for reference/proof of concept only.
-- Use fwa_downstreamlength() instead.

CREATE OR REPLACE FUNCTION fwa_lengthdownstream_recursive(
    blkey integer,
    measure double precision,
    padding numeric DEFAULT .001
)

RETURNS double precision AS $$
-- Note that this first CTE is not recursive, it just has to be marked as such
-- because the following CTE query is indeed recursive:
-- https://www.postgresql.org/message-id/13122.1339829536%%40sss.pgh.pa.us

-- find the watershed / local codes of stream segment where we want to start
WITH RECURSIVE start AS
(SELECT
    wscode_ltree,
    localcode_ltree,
    downstream_route_measure
  FROM whse_basemapping.fwa_stream_networks_sp
  WHERE blue_line_key = blkey
  AND downstream_route_measure < (measure - padding)
  ORDER BY downstream_route_measure desc
  LIMIT 1),

-- working downstream, recursively extract the watershed / local codes
dnst AS
  (SELECT
     str.wscode_ltree,
     str.localcode_ltree,
     str.downstream_route_measure,
     str.length_metre
   FROM
     whse_basemapping.fwa_stream_networks_sp str,
     start
   WHERE str.wscode_ltree = start.wscode_ltree
   AND str.localcode_ltree <= start.localcode_ltree
   AND str.downstream_route_measure <= start.downstream_route_measure
  UNION ALL
  SELECT
   str.wscode_ltree,
   str.localcode_ltree,
   str.downstream_route_measure,
   str.length_metre
 FROM
   whse_basemapping.fwa_stream_networks_sp str,
   -- grab only the last/bottom wscode from the recursive downstream query
   (SELECT wscode_ltree, localcode_ltree
    FROM dnst
    ORDER BY wscode_ltree DESC, localcode_ltree DESC LIMIT 1) AS b
 WHERE
   -- the watershed code is the parent of the previous watershed code
   str.wscode_ltree = subltree(b.wscode_ltree, 0, (nlevel(b.wscode_ltree) - 1))
   -- and the final segment of the local code is less than the final segment
   -- of the previous watershed code
   AND
   (subltree(str.localcode_ltree, (nlevel(str.localcode_ltree) - 1), nlevel(str.localcode_ltree))
     < subltree(b.wscode_ltree, (nlevel(b.wscode_ltree) - 1), nlevel(b.wscode_ltree))
   OR
   str.localcode_ltree = str.wscode_ltree)
 )

SELECT SUM(length_metre) + (measure - (SELECT downstream_route_measure FROM start))
FROM dnst

$$
language 'sql' immutable strict parallel safe;

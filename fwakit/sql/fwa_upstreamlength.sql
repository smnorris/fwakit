-- fwa_upstreamlength()
-- Provided blue_line_key and downstream_route_measure, return length upstream

CREATE OR REPLACE FUNCTION fwa_upstreamlength(
    blkey integer,
    measure double precision,
    padding numeric DEFAULT .001
)

RETURNS double precision AS $$

-- get the segment of interest
WITH a AS
  (SELECT * FROM whse_basemapping.fwa_stream_networks_sp
   WHERE
     blue_line_key = blkey
     AND downstream_route_measure <= (measure + padding)
   ORDER BY downstream_route_measure DESC
   LIMIT 1),

-- find everything upstream
upstream AS
(SELECT
 SUM(b.length_metre) as length_metre
FROM a
LEFT OUTER JOIN whse_basemapping.fwa_stream_networks_sp b
ON
  -- b is a child of a, always
  b.wscode_ltree <@ a.wscode_ltree
AND
    -- conditional upstream join logic, based on whether watershed codes are equivalent
  CASE
    -- first, consider simple case - streams where wscode and localcode are equivalent
    -- this is all segments with equivalent bluelinekey and a larger measure
    -- (plus fudge factor)
     WHEN
        a.wscode_ltree = a.localcode_ltree AND
        (
            (b.blue_line_key <> a.blue_line_key OR
             b.downstream_route_measure > a.downstream_route_measure + padding)
        )
     THEN TRUE
     -- next, the more complicated case - where wscode and localcode are not equal
     WHEN
        a.wscode_ltree != a.localcode_ltree AND
        (
         -- higher up the blue line (plus fudge factor)
            (b.blue_line_key = a.blue_line_key AND
             b.downstream_route_measure > a.downstream_route_measure + padding)
            OR
         -- tributaries: b wscode > a localcode and b wscode is not a child of a localcode
            (b.wscode_ltree > a.localcode_ltree AND
             NOT b.wscode_ltree <@ a.localcode_ltree)
            OR
         -- capture side channels: b is the same watershed code, with larger localcode
            (b.wscode_ltree = a.wscode_ltree
             AND b.localcode_ltree >= a.localcode_ltree)
        )
      THEN TRUE
  END
)

-- add together length from segment on which measure falls, plus
-- everything upstream
  SELECT (measure - a.downstream_route_measure) + upstream.length_metre
  FROM a, upstream;

$$
language 'sql' immutable strict parallel safe;

-- fwa_downstreamlength()

-- Return length of stream downstream of a point, given blue_line_key and
-- downstream_route_measure.
-- Note that where possible this returns side channels/distributaries as well
-- as main-stems.

CREATE OR REPLACE FUNCTION fwa_downstreamlength(
    blkey integer,
    measure double precision,
    padding numeric DEFAULT .001
)

RETURNS double precision AS $$

-- find the watershed / local codes of stream segment where we want to start
WITH a AS
(SELECT
    blue_line_key,
    wscode_ltree,
    localcode_ltree,
    downstream_route_measure
  FROM whse_basemapping.fwa_stream_networks_sp
  WHERE blue_line_key = blkey
  AND downstream_route_measure < (measure - padding)
  ORDER BY downstream_route_measure desc
  LIMIT 1),

-- find downstream segments
dnst AS
  (SELECT
     b.wscode_ltree,
     b.localcode_ltree,
     b.downstream_route_measure,
     b.length_metre
   FROM
     whse_basemapping.fwa_stream_networks_sp b
   INNER JOIN a ON
    -- donwstream criteria 1 - same blue line, lower measure
    (b.blue_line_key = a.blue_line_key AND
     b.downstream_route_measure <= a.downstream_route_measure)
    OR
    -- criteria 2 - watershed code a is a child of watershed code b
    (b.wscode_ltree @> a.wscode_ltree
        AND (
             -- AND local code is lower
             b.localcode_ltree < subltree(a.localcode_ltree, 0, nlevel(b.localcode_ltree))
             -- OR wscode and localcode are equivalent
             OR b.wscode_ltree = b.localcode_ltree
             -- OR any missed side channels on the same watershed code
             OR (b.wscode_ltree = a.wscode_ltree AND
                 b.blue_line_key != a.blue_line_key AND
                 b.localcode_ltree < a.localcode_ltree)
             )
    )
    )

SELECT SUM(length_metre) + (measure - (SELECT downstream_route_measure FROM a))
FROM dnst

$$
language 'sql' immutable strict parallel safe;

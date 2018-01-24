-- fwa_lengthinstream()

-- Return length of stream between two points

CREATE OR REPLACE FUNCTION fwa_lengthinstream(
    blkey_a integer,
    measure_a double precision,
    blkey_b integer,
    measure_b double precision,
    padding numeric DEFAULT .001
)

RETURNS double precision AS $$


-- find the watershed / local codes of starting (lower) point (a)
WITH a AS
(SELECT
    blue_line_key,
    wscode_ltree,
    localcode_ltree,
    length_metre,
    downstream_route_measure
  FROM whse_basemapping.fwa_stream_networks_sp
  WHERE blue_line_key = blkey_a
  AND downstream_route_measure < (measure_a - padding)
  ORDER BY downstream_route_measure desc
  LIMIT 1),

-- find the watershed / local codes of ending (higher) point (b)
b AS
(SELECT
    blue_line_key,
    wscode_ltree,
    localcode_ltree,
    length_metre,
    downstream_route_measure
  FROM whse_basemapping.fwa_stream_networks_sp
  WHERE blue_line_key = blkey_b
  AND downstream_route_measure < (measure_b - padding)
  ORDER BY downstream_route_measure desc
  LIMIT 1),

-- Find all stream segments between a and b by searching upstream of a
-- and downstream of b
instream as
(
  SELECT
    SUM(str.length_metre) as length_metre
  FROM
     whse_basemapping.fwa_stream_networks_sp str

  -- DOWNSTREAM JOIN
  INNER JOIN b ON
    -- donwstream criteria 1 - same blue line, lower measure
    (str.blue_line_key = b.blue_line_key AND
     str.downstream_route_measure <= b.downstream_route_measure)
    OR
    -- criteria 2 - watershed code a is a child of watershed code b
    (str.wscode_ltree @> b.wscode_ltree
        AND (
             -- AND local code is lower
             str.localcode_ltree < subltree(b.localcode_ltree, 0, nlevel(str.localcode_ltree))
             -- OR wscode and localcode are equivalent
             OR str.wscode_ltree = str.localcode_ltree
             -- OR any missed side channels on the same watershed code
             OR (str.wscode_ltree = b.wscode_ltree AND
                 str.blue_line_key != b.blue_line_key AND
                 str.localcode_ltree < b.localcode_ltree)
             )
    )

  -- UPSTREAM JOIN
  INNER JOIN a ON
    -- b is a child of a, always
    str.wscode_ltree <@ a.wscode_ltree
    AND
        -- conditional upstream join logic, based on whether watershed codes are equivalent
      CASE
        -- first, consider simple case - streams where wscode and localcode are equivalent
        -- this is all segments with equivalent bluelinekey and a larger measure
        -- (plus fudge factor)
         WHEN
            a.wscode_ltree = a.localcode_ltree AND
            (
                (str.blue_line_key <> a.blue_line_key OR
                 str.downstream_route_measure > a.downstream_route_measure + padding)
            )
         THEN TRUE
         -- next, the more complicated case - where wscode and localcode are not equal
         WHEN
            a.wscode_ltree != a.localcode_ltree AND
            (
             -- higher up the blue line (plus fudge factor)
                (str.blue_line_key = a.blue_line_key AND
                 str.downstream_route_measure > a.downstream_route_measure + padding)
                OR
             -- tributaries: b wscode > a localcode and b wscode is not a child of a localcode
                (str.wscode_ltree > a.localcode_ltree AND
                 NOT str.wscode_ltree <@ a.localcode_ltree)
                OR
             -- capture side channels: b is the same watershed code, with larger localcode
                (str.wscode_ltree = a.wscode_ltree
                 AND str.localcode_ltree >= a.localcode_ltree)
            )
          THEN TRUE
      END
)

-- Find actual length by removing the bottom end of the stream on which a lies,
-- and the top end of the stream on which b lies
SELECT
  i.length_metre -
    (measure_b - b.downstream_route_measure) -
    ((a.downstream_route_measure + a.length_metre) - measure_a) AS result
FROM instream i, a, b

$$
language 'sql' immutable strict parallel safe;
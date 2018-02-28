WITH a AS
  (SELECT
     linear_feature_id,
     blue_line_key,
     downstream_route_measure,
     wscode_ltree,
     localcode_ltree,
     ST_LineSubstring((ST_Dump(geom)).geom, ((%s - downstream_route_measure) / length_metre), 1) as geom
   FROM whse_basemapping.fwa_stream_networks_sp
   WHERE
     blue_line_key = %s
     AND downstream_route_measure <= (%s + .001)
     AND localcode_ltree IS NOT NULL AND localcode_ltree != ''
   ORDER BY downstream_route_measure DESC
   LIMIT 1),

upstream AS
(
  SELECT
    b.blue_line_key,
    b.geom
  FROM a
  LEFT OUTER JOIN whse_basemapping.fwa_stream_networks_sp b ON
    b.wscode_ltree <@ a.wscode_ltree
  AND b.linear_feature_id != a.linear_feature_id
  AND
    CASE
       WHEN
          a.wscode_ltree = a.localcode_ltree AND
          (
              (b.blue_line_key <> a.blue_line_key OR
               b.downstream_route_measure > a.downstream_route_measure + .001)
          )
       THEN TRUE
       WHEN
          a.wscode_ltree != a.localcode_ltree AND
          (
              (b.blue_line_key = a.blue_line_key AND
               b.downstream_route_measure > a.downstream_route_measure + .001)
              OR
              (b.wscode_ltree > a.localcode_ltree AND
               NOT b.wscode_ltree <@ a.localcode_ltree)
              OR
              (b.wscode_ltree = a.wscode_ltree
               AND b.localcode_ltree >= a.localcode_ltree)
          )
        THEN TRUE
    END
)

SELECT blue_line_key, geom
FROM a
UNION ALL
SELECT blue_line_key, geom
FROM upstream;
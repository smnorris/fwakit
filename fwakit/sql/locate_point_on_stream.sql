/*
locate_point_on_stream.sql

Return closest point to given feature on specified stream.

Inputs
 - remap: {"inputPointTable": point_table,
           "inputPointId": point_id}
 - param: (pointid, fwa_watershed_code, tolerance)

Returns closest measure on stream to point

*/

WITH ns AS
  (SELECT stream.linear_feature_id,
          stream.length_metre,
          stream.blue_line_key,
          stream.fwa_watershed_code,
          stream.local_watershed_code,
          stream.watershed_group_code,
          stream.downstream_route_measure,
          ST_ClosestPoint(stream.geom, point.geom) AS intersection,
          ST_Distance(stream.geom, point.geom) dist_to_pt
   FROM whse_basemapping.fwa_stream_networks_sp stream,
     (SELECT geom
      FROM $inputPointTable
      WHERE $inputPointId = %s) AS point
   WHERE stream.fwa_watershed_code = %s
     AND blue_line_key = watershed_key
     AND ST_DWithin(stream.geom, point.geom, %s)
   ORDER BY dist_to_pt
   LIMIT 1)

SELECT
  ST_Line_Locate_Point(
              (SELECT st_linemerge(geom) AS geom
               FROM whse_basemapping.fwa_stream_networks_sp AS foo
               WHERE linear_feature_id = ns.linear_feature_id), ns.intersection) * ns.length_metre + ns.downstream_route_measure
     AS downstream_route_measure,
  ns.linear_feature_id,
  ns.blue_line_key,
  ns.fwa_watershed_code,
  ns.local_watershed_code,
  ns.watershed_group_code,
  ns.dist_to_pt
FROM ns
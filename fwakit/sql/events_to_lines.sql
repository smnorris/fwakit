CREATE TABLE $outputTable AS
SELECT events.*,
       ST_LineSubstring(streams.geom,
                        ROUND(CAST((events.downstream_route_measure - streams.downstream_route_measure) / streams.length_metre AS NUMERIC), 5),
                        ROUND(CAST(((events.downstream_route_measure + events.length_metre) - streams.downstream_route_measure) / streams.length_metre AS NUMERIC), 5)) AS geom
  FROM $inputTable events
 INNER JOIN whse_basemapping.fwa_stream_networks_sp streams
         ON events.linear_feature_id = streams.linear_feature_id
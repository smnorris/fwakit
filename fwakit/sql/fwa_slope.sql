-- Return slope of a stream between two measures
-- fwa_slope(blue_line_key, downstream_route_measure, upstream_route_measure)

CREATE OR REPLACE FUNCTION fwa_slope(
    blkey integer,
    measure_down double precision,
    measure_up double precision
)

RETURNS numeric AS $$

SELECT
  ROUND(
   ((fwa_elevation(blkey, measure_up) - fwa_elevation(blkey, measure_down))
     /  ABS(measure_up - measure_down))::numeric * 100, 2);

$$
language 'sql' immutable strict parallel safe;
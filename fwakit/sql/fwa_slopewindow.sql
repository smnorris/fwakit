CREATE OR REPLACE FUNCTION fwa_slopewindow(
    blkey integer,
    measure double precision,
    window_size integer
)
RETURNS numeric

LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
  min_m double precision;
  max_m double precision;
  win double precision;
  slope numeric;

BEGIN

-- the function gets the full length across which we want to calculate the slope, but
-- all calculations just use half of this (distance from measure to end of window)
win := window_size / 2;

SELECT
  min(downstream_route_measure) as min_measure,
  max(upstream_route_measure) as max_measure
FROM whse_basemapping.fwa_stream_networks_sp
WHERE blue_line_key = blkey
INTO min_m, max_m;

IF min_m > measure OR max_m < measure THEN
  RAISE EXCEPTION 'Invalid measure, does not exist on stream';
END IF;

-- If the stream is actually shorter than provided length, calc slope over entire line
IF (max_m - min_m) < window_size
THEN
  SELECT INTO slope
    fwa_slope(blkey, min_m, max_m) as slope
  FROM measures;

-- Otherwise, check the measurement window - ensuring that we always measure the slope
-- over the lenght specified, even if the measure is closer to the end of the line than
-- the length really allows for
ELSE

WITH measures AS
(
  SELECT

    -- adjust bottom if necessary
    CASE
      -- bump the bottom down if we are near the end of the stream
      WHEN measure > (max_m - win) THEN max_m - (win * 2)
      -- bump the bottom up if we are near the start of the stream
      WHEN measure < (min_m + win) THEN min_m
      ELSE measure - win
    END as measure_down,

    -- adjust top if necessary
    CASE
      -- bump the top down if we are near the end of the stream
      WHEN (measure + win) > max_m THEN max_m
      -- bump the top up if we are near the start of the stream
      WHEN measure < (min_m + win) THEN min_m + (win * 2)
      ELSE measure + win
    END as measure_up
)

SELECT INTO slope
  fwa_slope(blkey, measure_down, measure_up) as slope
FROM measures;

END IF;

RETURN slope;
END; $BODY$;

-- Find stream slope at a given position, over the specified length interval (m).

-- This function calls fwa_slope(blue_line_key, measure1, measure2), but ensures that
-- the measures used are within the length of the stream - intervals that correspond to
-- measures shorter or longer than the stream are shifted to fall at the start/end
-- of the line (the length of the interval is preserved).

--  For example, on a hypothetical stream of blue_line_key=999999999 and length 1000:

--  Basic case, no shift of interval:
--    fwa_intervalslope(999999999, 350, 100)  ==  fwa_slope(999999999, 300, 400)

--  Provided interval would result in measure < 0, interval shifted to start at 0:
--    fwa_intervalslope(999999999, 20, 100)  ==  fwa_slope(999999999, 0, 100)

--  Interval starts at 0, no shift:
--    fwa_intervalslope(999999999, 50, 100)  ==  fwa_slope(999999999, 0, 100)

--  Provided interval would fall outside maximum length of stream, shifted to finish
--  at end of line while preserving length of interval:
--    fwa_intervalslope(999999999, 980, 100)  ==  fwa_slope(999999999, 950, 1000)

CREATE OR REPLACE FUNCTION fwa_slopewindow(
    blkey integer,             -- blue_line_key of stream
    measure double precision,  -- downstream_route_measure at centre of interval
    length integer             -- total length of interval/window to calculate the slope
)

RETURNS numeric

LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
  min_m numeric;
  max_m numeric;
  meas numeric;
  half_length double precision;
  slope numeric;

BEGIN

-- Most calculations just use half of the interval length
half_length := length / 2;

-- round the measure
meas := round(measure::numeric, 8);

SELECT
  -- round to avoid floating point issues
  -- a better strategy might be to round to the nearest cm/mm and convert to integer?
  -- this would require modifying fwa_slope as well
  min(round(downstream_route_measure::numeric, 8)) as min_measure,
  max(round(upstream_route_measure::numeric, 8)) as max_measure
FROM whse_basemapping.fwa_stream_networks_sp
WHERE blue_line_key = blkey
INTO min_m, max_m;

-- Check that the provided measure actually falls on the stream
IF meas < min_m OR meas > max_m THEN
  RAISE EXCEPTION 'Invalid measure, does not exist on stream';
END IF;

-- If the stream is actually shorter than provided interval, calc slope over entire line
IF (max_m - min_m) < length
THEN
  SELECT INTO slope
    fwa_slope(blkey, min_m, max_m) as slope;

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
      WHEN meas > (max_m - half_length) THEN max_m - (half_length * 2)
      -- bump the bottom up if we are near the start of the stream
      WHEN meas < (min_m + half_length) THEN min_m
      ELSE meas - half_length
    END as meas_down,

    -- adjust top if necessary
    CASE
      -- bump the top down if we are near the end of the stream
      WHEN (meas + half_length) > max_m THEN max_m
      -- bump the top up if we are near the start of the stream
      WHEN meas < (min_m + half_length) THEN min_m + (half_length * 2)
      ELSE meas + half_length
    END as meas_up
)

SELECT INTO slope
  fwa_slope(blkey, meas_down, meas_up) as slope
FROM measures;

END IF;

RETURN slope;
END; $BODY$;

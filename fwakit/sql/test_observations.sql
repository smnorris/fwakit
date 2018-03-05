-- ---------------------------------------------
-- Reference fish observations on the FWA stream network, creating output
-- event table linking the observation points to blue_lines
-- Building the observation event table is more complex than simply snapping
-- to the nearest stream because we want to ensure that observations within
-- waterbodies are associated with waterbodies, rather than the closest stream
-- ---------------------------------------------



-- ---------------------------------------------
-- First, create preliminary event table, with observations matched to all
-- streams within 1000m. Use this for subsequent analysis. Since we are using
-- such a large search area and also calculating the measures, this may take
-- some time (~5min)
-- ---------------------------------------------
DROP TABLE IF EXISTS whse_fish.fiss_fish_obsrvtn_events_prelim1;

CREATE TABLE whse_fish.fiss_fish_obsrvtn_events_prelim1 AS
-- find nearest streams within 1000m, these are candidates for matching
WITH candidates AS
 ( SELECT
    pt.fiss_fish_obsrvtn_distinct_id,
    nn.linear_feature_id,
    nn.wscode_ltree,
    nn.localcode_ltree,
    nn.blue_line_key,
    nn.waterbody_key,
    nn.length_metre,
    nn.downstream_route_measure,
    nn.distance_to_stream,
    ST_LineMerge(nn.geom) AS geom
  FROM whse_fish.fiss_fish_obsrvtn_distinct as pt
  CROSS JOIN LATERAL
  (SELECT
     str.linear_feature_id,
     str.wscode_ltree,
     str.localcode_ltree,
     str.blue_line_key,
     str.waterbody_key,
     str.length_metre,
     str.downstream_route_measure,
     str.geom,
     ST_Distance(str.geom, pt.geom) as distance_to_stream
    FROM whse_basemapping.fwa_stream_networks_sp AS str
    WHERE str.localcode_ltree IS NOT NULL
    AND NOT str.wscode_ltree <@ '999'
    ORDER BY str.geom <-> pt.geom
    LIMIT 100) as nn
  WHERE nn.distance_to_stream < 1000
),

-- find just the closest point for distinct blue_line_keys -
-- we don't want to match to all individual stream segments
bluelines AS
(SELECT DISTINCT ON (fiss_fish_obsrvtn_distinct_id, blue_line_key)
  fiss_fish_obsrvtn_distinct_id,
  blue_line_key,
  distance_to_stream
FROM candidates
ORDER BY fiss_fish_obsrvtn_distinct_id, blue_line_key, distance_to_stream
)

-- from the selected blue lines, generate downstream_route_measure
SELECT
  bluelines.fiss_fish_obsrvtn_distinct_id,
  candidates.linear_feature_id,
  candidates.wscode_ltree,
  candidates.localcode_ltree,
  candidates.waterbody_key,
  bluelines.blue_line_key,
  (ST_LineLocatePoint(candidates.geom,
                       ST_ClosestPoint(candidates.geom, pts.geom))
     * candidates.length_metre) + candidates.downstream_route_measure
    AS downstream_route_measure,
  candidates.distance_to_stream
FROM bluelines
INNER JOIN candidates ON bluelines.fiss_fish_obsrvtn_distinct_id = candidates.fiss_fish_obsrvtn_distinct_id
AND bluelines.blue_line_key = candidates.blue_line_key
AND bluelines.distance_to_stream = candidates.distance_to_stream
INNER JOIN whse_fish.fiss_fish_obsrvtn_distinct pts ON bluelines.fiss_fish_obsrvtn_distinct_id = pts.fiss_fish_obsrvtn_distinct_id;

-- ---------------------------------------------
-- index the intermediate table
CREATE INDEX ON whse_fish.fiss_fish_obsrvtn_events_prelim1 (fiss_fish_obsrvtn_distinct_id);

-- ---------------------------------------------
-- Create empty output table
DROP TABLE IF EXISTS whse_fish.fiss_fish_obsrvtn_events_prelim2;

CREATE TABLE whse_fish.fiss_fish_obsrvtn_prelim2 AS
SELECT * FROM whse_fish.fiss_fish_obsrvtn_events_prelim1 LIMIT 0;


-- ---------------------------------------------
-- Insert events matched to waterbodies.
-- This is probably a lot more complicated than it has to be but we want to
-- ensure that observations in waterbodies are associated with waterbodies
-- rather than just the closest stream. We use a large 1000m tolerance (in above
-- query) because observations in lakes may well be quite far from a stream flow
-- line within larger lakes, or coordinates may be well away from the lake.
-- This query:
--   - joins the observations to the FWA via wbody_key (1:many via lookup)
--   - from the many possible matches, choose just the closest
--   - inserts these records into the output table

-- where observation is coded as a lake or wetland,
-- join to waterbody_key via wdic_waterbodies
WITH wb AS
(
  SELECT DISTINCT
    o.fiss_fish_obsrvtn_distinct_id,
    wb.waterbody_key
  FROM whse_fish.fiss_fish_obsrvtn_distinct o
  INNER JOIN whse_fish.wdic_waterbodies wdic ON o.wbody_id = wdic.id
  INNER JOIN whse_basemapping.fwa_waterbodies_20k_50k lut
     ON LTRIM(wdic.waterbody_identifier,'0') = lut.waterbody_key_50k::TEXT||lut.watershed_group_code_50k
  INNER JOIN
     (SELECT DISTINCT waterbody_key, watershed_group_code
      FROM whse_basemapping.fwa_lakes_poly
      UNION ALL
      SELECT DISTINCT waterbody_key, watershed_group_code
      FROM whse_basemapping.fwa_manmade_waterbodies_poly
      UNION ALL
      SELECT DISTINCT waterbody_key, watershed_group_code
      FROM whse_basemapping.fwa_wetlands_poly
      ) wb
  ON lut.waterbody_key_20k = wb.waterbody_key
  WHERE o.waterbody_type IN ('Lake', 'Wetland')
  ORDER BY o.fiss_fish_obsrvtn_distinct_id
),
-- from the candidate matches generated above, use the one closest to a stream
closest AS
(
  SELECT DISTINCT ON
   (e.fiss_fish_obsrvtn_distinct_id)
    e.fiss_fish_obsrvtn_distinct_id,
    e.distance_to_stream
  FROM whse_fish.fiss_fish_obsrvtn_events_prelim1 e
  INNER JOIN wb ON e.fiss_fish_obsrvtn_distinct_id = wb.fiss_fish_obsrvtn_distinct_id
  AND e.waterbody_key = wb.waterbody_key
  ORDER BY fiss_fish_obsrvtn_distinct_id, distance_to_stream
)
-- insert the results into our output table
-- there are duplicate records due to various quirks in the data (streams can
-- potentially be equidistant from an observation). Insert only
-- distinct records
INSERT INTO whse_fish.fiss_fish_obsrvtn_events_prelim2
SELECT DISTINCT e.*
FROM whse_fish.fiss_fish_obsrvtn_events_prelim1 e
INNER JOIN closest
ON e.fiss_fish_obsrvtn_distinct_id = closest.fiss_fish_obsrvtn_distinct_id
AND e.distance_to_stream = closest.distance_to_stream
WHERE e.waterbody_key is NOT NULL;


-- ---------------------------------------------
-- Some observations in waterbodies do not get added above due to lookup quirks.
-- Insert these records simply based on the closest stream
-- ---------------------------------------------
WITH unmatched_wb AS
(    SELECT e.*
    FROM whse_fish.fiss_fish_obsrvtn_events_prelim1 e
    INNER JOIN whse_fish.fiss_fish_obsrvtn_distinct o
    ON e.fiss_fish_obsrvtn_distinct_id = o.fiss_fish_obsrvtn_distinct_id
    LEFT OUTER JOIN whse_fish.fiss_fish_obsrvtn_events_prelim2 p
    ON e.fiss_fish_obsrvtn_distinct_id = p.fiss_fish_obsrvtn_distinct_id
    WHERE o.wbody_id IS NOT NULL AND o.waterbody_type IN ('Lake','River')
    AND p.fiss_fish_obsrvtn_distinct_id IS NULL
),
closest_unmatched AS

(
  SELECT DISTINCT ON (fiss_fish_obsrvtn_distinct_id)
    fiss_fish_obsrvtn_distinct_id,
    distance_to_stream
  FROM unmatched_wb
  ORDER BY fiss_fish_obsrvtn_distinct_id, distance_to_stream
)

INSERT INTO whse_fish.fiss_fish_obsrvtn_events_prelim2
SELECT DISTINCT e.*
FROM whse_fish.fiss_fish_obsrvtn_events_prelim1 e
INNER JOIN closest_unmatched
ON e.fiss_fish_obsrvtn_distinct_id = closest_unmatched.fiss_fish_obsrvtn_distinct_id
AND e.distance_to_stream = closest_unmatched.distance_to_stream;


-- ---------------------------------------------
-- All observations in waterbodies should now be in the output.
-- Next, insert observations in streams.
-- We *could* use the 50k-20k lookup to restrict matches but matching to the
-- nearest stream is probably adequate for this exercise.
-- Note that our tolerance is much smaller than with waterbodies -
-- extract only records within 300m of a stream from the preliminary table.
-- ---------------------------------------------
WITH unmatched AS
(   SELECT e.*
    FROM whse_fish.fiss_fish_obsrvtn_events_prelim1 e
    INNER JOIN whse_fish.fiss_fish_obsrvtn_distinct o
    ON e.fiss_fish_obsrvtn_distinct_id = o.fiss_fish_obsrvtn_distinct_id
    WHERE o.waterbody_type NOT IN ('Lake','River')
    AND e.distance_to_stream <= 300
),

closest_unmatched AS
(
  SELECT DISTINCT ON (fiss_fish_obsrvtn_distinct_id)
    fiss_fish_obsrvtn_distinct_id,
    distance_to_stream
  FROM unmatched
  ORDER BY fiss_fish_obsrvtn_distinct_id, distance_to_stream
)

INSERT INTO whse_fish.fiss_fish_obsrvtn_events_prelim2
SELECT DISTINCT e.*
FROM whse_fish.fiss_fish_obsrvtn_events_prelim1 e
INNER JOIN closest_unmatched
ON e.fiss_fish_obsrvtn_distinct_id = closest_unmatched.fiss_fish_obsrvtn_distinct_id
AND e.distance_to_stream = closest_unmatched.distance_to_stream;


-- Clean up the output, removing locations that are duplicates. Duplicates
-- are generally when a point has been matched to a location where two
-- streams meet. Remove the watershed codes etc to avoid duplication -
-- join back to the streams to do upstream downstream queries.
CREATE TABLE whse_fish.fiss_fish_obsrvtn_events AS
SELECT DISTINCT
  fiss_fish_obsrvtn_distinct_id,
  blue_line_key,
  downstream_route_measure,
  distance_to_stream
FROM whse_fish.fiss_fish_obsrvtn_events_prelim2;

CREATE INDEX ON whse_fish.fiss_fish_obsrvtn_events (fiss_fish_obsrvtn_distinct_id);

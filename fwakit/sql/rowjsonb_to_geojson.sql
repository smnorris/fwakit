-- http://blog.cleverelephant.ca/2019/03/geojson.html
CREATE OR REPLACE FUNCTION rowjsonb_to_geojson(
  rowjsonb JSONB,
  geom_column TEXT DEFAULT 'geom')
RETURNS TEXT AS
$$
DECLARE
 json_props jsonb;
 json_geom jsonb;
 json_type jsonb;
BEGIN
 IF NOT rowjsonb ? geom_column THEN
   RAISE EXCEPTION 'geometry column ''%'' is missing', geom_column;
 END IF;
 json_geom := ST_AsGeoJSON((rowjsonb ->> geom_column)::geometry)::jsonb;
 json_geom := jsonb_build_object('geometry', json_geom);
 json_props := jsonb_build_object('properties', rowjsonb - geom_column);
 json_type := jsonb_build_object('type', 'Feature');
 return (json_type || json_geom || json_props)::text;
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE STRICT;
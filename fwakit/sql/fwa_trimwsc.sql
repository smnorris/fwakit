-- ------------------------------
-- trim trailing -000000- characters from a watershed code
-- ------------------------------
CREATE OR REPLACE FUNCTION fwa_trimwsc(text)
  RETURNS text
AS $$
SELECT regexp_replace($1, '(\-000000)+$', ''); $$
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;
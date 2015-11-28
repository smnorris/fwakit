-- ------------------------------
-- trim trailing -000000- characters from FWA watershed codes
-- ------------------------------
CREATE OR REPLACE FUNCTION public.wscode_trim(text)
  RETURNS text
AS $$
SELECT regexp_replace($1, '(\-000000)+$', ''); $$
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;
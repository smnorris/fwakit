-- ------------------------------
-- convert a watershed code string to ltree
-- ------------------------------
CREATE OR REPLACE FUNCTION fwa_wsc2ltree(text)
  RETURNS ltree
AS $$

SELECT
  text2ltree(replace(regexp_replace($1, '(\-000000)+$', ''), '-', '.'));

$$
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;
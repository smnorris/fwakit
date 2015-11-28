-- add ltree columns to a table
-- any existing indexes/constraints/etc in source will need to be recreated
CREATE TABLE $schema.temp_ltree_copy AS
SELECT
  *,
  CASE WHEN POSITION('-' IN wscode_trim(fwa_watershed_code)) > 0
            THEN text2ltree(REPLACE(wscode_trim(fwa_watershed_code),'-','.'))
       ELSE  text2ltree(wscode_trim(fwa_watershed_code))
  END as wscode_ltree,
  CASE WHEN POSITION('-' IN wscode_trim(local_watershed_code)) > 0
            THEN text2ltree(REPLACE(wscode_trim(local_watershed_code),'-','.'))
       ELSE  text2ltree(wscode_trim(local_watershed_code))
  END as localcode_ltree
FROM $schema.$sourceTable;

DROP TABLE $schema.$sourceTable;

ALTER TABLE $schema.temp_ltree_copy RENAME TO $sourceTable;

CREATE INDEX $sourceTable_wslt_btree ON $schema.$sourceTable USING btree (wscode_ltree);
CREATE INDEX $sourceTable_wslt_gist ON $schema.$sourceTable USING gist (wscode_ltree);
CREATE INDEX $sourceTable_locallt_btree ON $schema.$sourceTable USING btree (localcode_ltree);
CREATE INDEX $sourceTable_locallt_gist ON $schema.$sourceTable USING gist (localcode_ltree);
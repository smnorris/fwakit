CREATE TABLE IF NOT EXISTS whse_basemapping.fwa_streams_20k_50k_wsc AS
   SELECT DISTINCT
     watershed_code_50k,
     fwa_watershed_code_20k,
     watershed_group_code_20k
   FROM whse_basemapping.fwa_streams_20k_50k
   ORDER BY watershed_code_50k, fwa_watershed_code_20k;

CREATE INDEX ON whse_basemapping.fwa_streams_20k_50k_wsc (watershed_code_50k);
CREATE INDEX ON whse_basemapping.fwa_streams_20k_50k_wsc (fwa_watershed_code_20k);
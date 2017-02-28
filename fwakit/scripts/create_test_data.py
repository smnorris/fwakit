# create a test data package from an existing fwa database
import os
import fwakit

fwa = fwakit.FWA()
source_files = ["FWA_BC.gdb.zip", "FWA_STREAM_NETWORKS_SP.gdb.zip"]
source_tables = ["fwa_stream_networks_sp", "fwa_watershed_groups_poly",
                 "fwa_lakes_poly"]

for source_file in fwa.config['source_files']:
    for table in fwa.config['source_files'][source_file]:
        if source_file in source_files and table in source_tables:
            out_file = os.path.splitext(source_file)[0]
            sql = "SELECT geometrytype(geom) FROM {t} LIMIT 1".format(t=fwa.tables[table])
            geom_type = fwa.db.query(sql).fetchone()[0]
            columns = fwa.db[fwa.tables[table]].columns
            columns = [c for c in columns if 'ltree' not in c]
            sql = """SELECT {c} FROM {t}
                     WHERE watershed_group_code = 'VICT'
                  """.format(c=', '.join(columns), t=fwa.tables[table])
            if 'grouped' not in fwa.config['source_files'][source_file][table].keys():
                outlayer = table
            else:
                outlayer = 'VICT'
            fwa.db.pg2ogr(sql, "FileGDB", out_file, outlayer=outlayer,
                          geom_type=geom_type)

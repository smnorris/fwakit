# global config/settings
# modify by passing values to utils.config()

import json
import logging as lg
import pkg_resources

# where to download FWA source data from
source_url = r'ftp://ftp.geobc.gov.bc.ca/sections/outgoing/bmgs/FWA_Public/'

# where to download FWA source data to
dl_path = 'fwakit_downloads'

# logging info
log_level = lg.INFO
log_file = 'True'
log_console = 'False'
log_name = 'fwakit'
log_filename = 'fwakit'
logs_folder = r'logs'

# columns to drop from source data
drop_columns = ['ogc_fid', 'objectid', 'geometry_area', 'geometry_length']

# source data definition dict stored as json
# dict keys are the table names, values are dicts with keys for:
# - alias (for shortcuts)
# - source file name
# - primary key (id)
# - additional fields to be indexed
# - whether table is 'grouped' (a table for each watershed group in the gdb)
source_tables = json.loads(pkg_resources.resource_string(__name__, "sources.json"))

# note distinct source files
source_files = list(set([f['source_file'] for f in source_tables]))

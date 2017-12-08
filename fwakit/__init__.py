from __future__ import absolute_import
import os
import logging

from fwakit import util

__version__ = "0.0.1"

config = util.read_config(os.path.join(os.path.dirname(__file__),
                          'config.yml'))
if config['log_level'] == 'INFO':
    config['log_level'] = logging.INFO
if config['log_level'] == 'WARNING':
    config['log_level'] = logging.WARNING
if config['log_level'] == 'DEBUG':
    config['log_level'] = logging.DEBUG

data_def = util.read_config(os.path.join(os.path.dirname(__file__),
                            config['data_def']))
queries = util.load_queries()

# define shortcuts to names of tables noted in data_def
tables = {}
aliases = {}
for source_file in data_def:
    for table in data_def[source_file]:
        t = data_def[source_file][table]
        a = data_def[source_file][table]['alias']
        # add schema qualified table name to table dict, for both
        # full table name and alias
        tables[table] = 'whse_basemapping.' + table
        aliases[a] = 'whse_basemapping.' + table
        # add the configured table def to config attributes
        config[table] = t

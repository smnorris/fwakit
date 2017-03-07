from __future__ import absolute_import
import os
try:
    from urllib.parse import urlparse
except ImportError:
     from urlparse import urlparse

from click.testing import CliRunner

import fwakit
from fwakit.scripts.cli import cli


CONFIG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_config.yml")
FWA = fwakit.FWA(config=CONFIG)

GROUPED_FILE = 'FWA_STREAM_NETWORKS_SP.gdb.zip'
GROUPED_LAYER = 'fwa_stream_networks_sp'

SIMPLE_FILE = 'FWA_BC.gdb.zip'
SIMPLE_LAYER = 'fwa_lakes_poly'


def test_download():
    runner = CliRunner()
    for f in [GROUPED_FILE, SIMPLE_FILE]:
        runner.invoke(cli, ['download', '-f', f])
        assert os.path.exists(os.path.join(FWA.config['dl_path'],
                                           os.path.splitext(f)[0]))


def test_load_grouped():
    runner = CliRunner()
    runner.invoke(cli, ['load', '-l', GROUPED_LAYER])
    assert GROUPED_LAYER in FWA.db.tables_in_schema(FWA.schema)


def test_load_simple():
    runner = CliRunner()
    runner.invoke(cli, ['load', '-l', SIMPLE_LAYER])
    assert SIMPLE_LAYER in FWA.db.tables_in_schema(FWA.schema)


def test_index():
    runner = CliRunner()
    runner.invoke(cli, ['index', '-l', GROUPED_LAYER])
    assert 'ogc_fid' not in FWA.db[FWA.schema+"."+GROUPED_LAYER].columns
    assert 'wscode_ltree' in FWA.db[FWA.schema+"."+GROUPED_LAYER].columns
    # todo, test for index presence
    #n_columns = len(FWA.config[GROUPED_LAYER]['index_fields'])
    #indexes = FWA.db[FWA.schema+"."+GROUPED_LAYER].indexes.keys()


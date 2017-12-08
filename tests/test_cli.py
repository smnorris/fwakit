from __future__ import absolute_import
import os
try:
    from urllib.parse import urlparse
except ImportError:
     from urlparse import urlparse

from click.testing import CliRunner

import fwakit as fwa
from fwakit.cli import cli

SOURCE_URL = 'http://www.hillcrestgeo.ca/outgoing/public/fwakit/'
DB_URL = 'postgresql://postgres:postgres@localhost:5432/fwakit_test'
DL_PATH = 'test_data'

GROUPED_FILE = 'FWA_STREAM_NETWORKS_SP.gdb.zip'
GROUPED_LAYER = 'fwa_stream_networks_sp'

SIMPLE_FILE = 'FWA_BC.gdb.zip'
SIMPLE_LAYER = 'fwa_lakes_poly'


def test_download():
    runner = CliRunner()
    for f in [GROUPED_FILE, SIMPLE_FILE]:
        runner.invoke(cli, ['download', '-f', f,
                                        '-u', SOURCE_URL,
                                        '-p', DL_PATH])
        assert os.path.exists(os.path.join(DL_PATH,
                                           os.path.splitext(f)[0]))


def test_load_grouped():
    runner = CliRunner()
    db = fwa.util.connect()
    runner.invoke(cli, ['load', '-l', GROUPED_LAYER,
                                '-p', DL_PATH])
    assert GROUPED_LAYER in db.tables_in_schema('whse_basemapping')


def test_load_simple():
    runner = CliRunner()
    db = fwa.util.connect()
    runner.invoke(cli, ['load', '-l', SIMPLE_LAYER,
                                '-p', DL_PATH])
    assert SIMPLE_LAYER in db.tables_in_schema('whse_basemapping')


def test_index():
    runner = CliRunner()
    runner.invoke(cli, ['index', '-l', GROUPED_LAYER])
    db = fwa.util.connect()
    assert 'ogc_fid' not in db['whse_basemapping.'+GROUPED_LAYER].columns
    assert 'wscode_ltree' in db['whse_basemapping.'+GROUPED_LAYER].columns
    # todo, test for index presence
    #n_columns = len(FWA.config[GROUPED_LAYER]['index_fields'])
    #indexes = FWA.db[FWA.schema+"."+GROUPED_LAYER].indexes.keys()


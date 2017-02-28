from __future__ import absolute_import
import os
try:
    from urllib.parse import urlparse
except ImportError:
     from urlparse import urlparse

from click.testing import CliRunner

import fwakit
from fwakit.scripts.cli import cli


TEST_FILE = 'FWA_STREAM_NETWORKS_SP.gdb.zip'
TEST_LAYER = 'fwa_stream_networks_sp'

FWA = fwakit.FWA()


def test_download():
    runner = CliRunner()
    runner.invoke(cli, ['download', '-f', TEST_FILE])
    assert os.path.exists(os.path.join(FWA.config['dl_path'],
                                       os.path.splitext(TEST_FILE)[0]))


def test_load():
    runner = CliRunner()
    runner.invoke(cli, ['load', '-l', TEST_LAYER])
    assert TEST_LAYER in FWA.db.tables_in_schema(FWA.schema)


def test_index():
    runner = CliRunner()
    runner.invoke(cli, ['index', '-l', TEST_LAYER])
    assert 'ogc_fid' not in FWA.db[FWA.schema+"."+TEST_LAYER].columns
    assert 'wscode_ltree' in FWA.db[FWA.schema+"."+TEST_LAYER].columns

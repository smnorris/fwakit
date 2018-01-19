from __future__ import absolute_import
import os

from click.testing import CliRunner

import fwakit as fwa
from fwakit.cli import cli


SOURCE_URL = 'https://www.hillcrestgeo.ca/outgoing/fwakit/'
DB_URL = os.environ['FWA_DB_TEST']
DL_PATH = 'test_data'

GROUPED_FILE = 'FWA_STREAM_NETWORKS_SP.gdb.zip'
GROUPED_LAYER = 'fwa_stream_networks_sp'
GROUP = 'VICT'

SIMPLE_FILE = 'FWA_BC.gdb.zip'
SIMPLE_LAYER = 'fwa_lakes_poly'


#def test_setUp():
#    runner = CliRunner()
#    runner.invoke(cli, ['create_db', '-db', DB_URL])
#    db = fwa.util.connect(DB_URL)
#    assert 'whse_basemapping' in db.schemas


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
    runner.invoke(cli, ['load', '-l', GROUPED_LAYER,
                                '-p', DL_PATH,
                                '-db', DB_URL,
                                '-g', GROUP])
    db = fwa.util.connect(DB_URL)
    assert GROUPED_LAYER in db.tables_in_schema('whse_basemapping')
    assert 'ogc_fid' not in db['whse_basemapping.'+GROUPED_LAYER].columns
    assert 'wscode_ltree' in db['whse_basemapping.'+GROUPED_LAYER].columns


def test_load_simple():
    runner = CliRunner()
    db = fwa.util.connect(DB_URL)
    runner.invoke(cli, ['load', '-l', SIMPLE_LAYER,
                                '-p', DL_PATH,
                                '-db', DB_URL])
    assert SIMPLE_LAYER in db.tables_in_schema('whse_basemapping')


def test_load_groups():
    runner = CliRunner()
    db = fwa.util.connect(DB_URL)
    runner.invoke(cli, ['load', '-l', 'fwa_watershed_groups_poly',
                                '-p', DL_PATH,
                                '-db', DB_URL])
    assert 'fwa_watershed_groups_poly' in db.tables_in_schema('whse_basemapping')
    assert 'fwa_watershed_groups_subdivided' in db.tables_in_schema('whse_basemapping')

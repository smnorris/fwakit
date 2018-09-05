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
GROUP = 'SALM'

SIMPLE_FILE = 'FWA_BC.gdb.zip'
SIMPLE_LAYER = 'fwa_lakes_poly'


def setup():
    runner = CliRunner()
    runner.invoke(cli, ['create_db', '-db', DB_URL])
    db = fwa.util.connect(DB_URL)
    assert 'public.spatial_ref_sys' in db.tables
    

def test_setUp():
    db = fwa.util.connect(DB_URL)
    db.create_schema('whse_basemapping')


def test_download():
    runner = CliRunner()
    for f in [GROUPED_FILE, SIMPLE_FILE]:
        runner.invoke(cli, ['download', '-f', f,
                                        '-u', SOURCE_URL,
                                        '-p', DL_PATH])
        assert os.path.exists(os.path.join(DL_PATH,
                                           os.path.splitext(f)[0]))


def test_load_groups():
    runner = CliRunner()
    db = fwa.util.connect(DB_URL)
    runner.invoke(cli, ['load', '-l', 'fwa_watershed_groups_poly',
                                '-p', DL_PATH,
                                '-db', DB_URL])
    assert 'fwa_watershed_groups_poly' in db.tables_in_schema('whse_basemapping')


def test_load_simple():
    runner = CliRunner()
    db = fwa.util.connect(DB_URL)
    runner.invoke(cli, ['load', '-l', SIMPLE_LAYER,
                                '-p', DL_PATH,
                                '-db', DB_URL])
    assert SIMPLE_LAYER in db.tables_in_schema('whse_basemapping')


def test_load_streams():
    runner = CliRunner()
    runner.invoke(cli, ['load', '-l', 'fwa_stream_networks_sp',
                                '-p', DL_PATH,
                                '-db', DB_URL,
                                '-g', GROUP])
    db = fwa.util.connect(DB_URL)
    assert 'fwa_stream_networks_sp' in db.tables_in_schema('whse_basemapping')


def test_load_watersheds():
    runner = CliRunner()
    runner.invoke(cli, ['load', '-l', 'fwa_watersheds_poly_sp',
                                '-p', DL_PATH,
                                '-db', DB_URL,
                                '-g', GROUP])
    db = fwa.util.connect(DB_URL)
    assert 'fwa_watersheds_poly_sp' in db.tables_in_schema('whse_basemapping')


def test_clean():
    runner = CliRunner()
    runner.invoke(cli, ['clean', '-l', 'fwa_stream_networks_sp,fwa_watersheds_poly_sp',
                                 '-db', DB_URL])
    db = fwa.util.connect(DB_URL)
    assert 'ogc_fid' not in db['whse_basemapping.fwa_stream_networks_sp'].columns
    assert 'objectid' not in db['whse_basemapping.fwa_stream_networks_sp'].columns
    assert 'wscode_ltree' in db['whse_basemapping.fwa_stream_networks_sp'].columns
    assert 'ogc_fid' not in db['whse_basemapping.fwa_watersheds_poly_sp'].columns
    assert 'objectid' not in db['whse_basemapping.fwa_watersheds_poly_sp'].columns
    assert 'wscode_ltree' in db['whse_basemapping.fwa_watersheds_poly_sp'].columns
    assert 'fwa_watershed_groups_subdivided' in db.tables_in_schema('whse_basemapping')

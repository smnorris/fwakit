from __future__ import absolute_import
import os

import fwakit as fwa


DB_URL = os.environ['FWA_DB_TEST']
GROUP = 'SALM'


def test_trim_ws_code():
    assert '920' == fwa.trim_ws_code('920-000000')
    assert '920-123456' == fwa.trim_ws_code('920-123456-000000')


def test_queries():
    assert fwa.queries['test'] == 'SELECT test'


def test_list_groups():
    db = fwa.util.connect(DB_URL)
    groups = fwa.list_groups(table='whse_basemapping.fwa_stream_networks_sp',
                             db=db)
    assert groups[0] == GROUP
    assert len(groups) == 1


def test_get_local_code():
    db = fwa.util.connect(DB_URL)
    assert (fwa.trim_ws_code(
            fwa.get_local_code(354154853, 31850, db=db)) == '920-722273-132687-611805')


def test_add_ltree():
    test_table = 'whse_basemapping.fwa_stream_networks_sp'
    test_column = 'wscode_ltree'
    db = fwa.util.connect(DB_URL)
    if test_column in db[test_table].columns:
        db[test_table].drop_column(test_column)
    fwa.add_ltree(test_table, {'fwa_watershed_code': test_column}, db=db)
    assert test_column in db[test_table].columns


def test_upstreamwsc_wsds_unequal_codes():
    db = fwa.util.connect(DB_URL)
    r = db.query("""SELECT COUNT(*)
                    FROM whse_basemapping.fwa_watersheds_poly_sp wsd
                    WHERE FWA_UpstreamWSC('920.722273'::ltree,
                                          '920.722273.097248'::ltree,
                                          wsd.wscode_ltree,
                                          wsd.localcode_ltree)
                 """).fetchone()
    assert r[0] == 3261


def test_upstreamwsc_wsds_equal_codes():
    db = fwa.util.connect(DB_URL)
    r = db.query("""SELECT COUNT(*)
                    FROM whse_basemapping.fwa_watersheds_poly_sp wsd
                    WHERE FWA_UpstreamWSC('920.705877'::ltree,
                                       '920.705877'::ltree,
                                       wsd.wscode_ltree,
                                       wsd.localcode_ltree)
                 """).fetchone()
    assert r[0] == 23

#def test_tearDown():
#    db = fwa.util.connect(DB_URL)
#    db.drop_schema('whse_basemapping', cascade=True)

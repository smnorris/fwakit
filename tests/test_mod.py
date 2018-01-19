from __future__ import absolute_import

import fwakit as fwa

DB_URL = 'postgresql://postgres:postgres@localhost:5432/fwakit'


def test_trim_ws_code():
    assert '920' == fwa.trim_ws_code('920-000000')
    assert '920-123456' == fwa.trim_ws_code('920-123456-000000')


def test_queries():
    assert fwa.queries['test'] == 'SELECT test'


def test_list_groups():
    db = fwa.util.connect(DB_URL)
    groups = fwa.list_groups(table='whse_basemapping.fwa_stream_networks_sp',
                             db=db)
    assert groups[0] == 'VICT'
    assert len(groups) == 1


def test_get_local_code():
    assert (fwa.trim_ws_code(fwa.get_local_code(354155107, 3400)) ==
            '920-076175-303123')


def test_add_ltree():
    test_table = 'whse_basemapping.fwa_stream_networks_sp'
    test_column = 'wscode_ltree'
    db = fwa.util.connect(DB_URL)
    if test_column in db[test_table].columns:
        db[test_table].drop_column(test_column)
    fwa.add_ltree(test_table, {'fwa_watershed_code': test_column}, db=db)
    assert test_column in db[test_table].columns

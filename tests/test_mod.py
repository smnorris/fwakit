from __future__ import absolute_import
import os

import fwakit

CONFIG = fwakit.config

# Set testing config params
CONFIG['db_url'] = 'postgresql://postgres:postgres@localhost:5432/fwa_test'

FWA = fwakit.FWA()


def test_initialize():
    assert FWA.invalid_streams[0] == 701241277


def test_trim_ws_code():
    assert '920' == FWA.trim_ws_code('920-000000')
    assert '920-123456' == FWA.trim_ws_code('920-123456-000000')


def test_list_groups():
    groups = FWA.list_groups(table=FWA.schema+'.fwa_stream_networks_sp')
    assert groups[0] == 'VICT'
    assert len(groups) == 1


def test_get_local_code():
    assert FWA.trim_ws_code(FWA.get_local_code(354155107, 3400)) == \
      '920-076175-303123'


def test_add_ltree():
    table = FWA.schema+".fwa_stream_networks_sp"
    if 'wscode_ltree' in FWA.db[table].columns:
        FWA.db.execute("ALTER TABLE {t} DROP COLUMN wscode_ltree".format(t=table))
    if 'localcode_ltree' in FWA.db[table].columns:
        FWA.db.execute("ALTER TABLE {t} DROP COLUMN localcode_ltree".format(t=table))
    FWA.add_ltree(FWA.schema+'.fwa_stream_networks_sp')
    assert 'wscode_ltree' in FWA.db[FWA.schema+".fwa_stream_networks_sp"].columns
    assert 'localcode_ltree' in FWA.db[FWA.schema+".fwa_stream_networks_sp"].columns

# this works
#def test_st_distance():
#    r = FWA.db.query("""SELECT ST_Distance(ST_GeomFromText('POINT(0 0)'),
#                                           ST_GeomFromText('POINT(0 10)'))""")

# but this doesn't, psycopg2 wants ST_Distance inputs cast explicitly
# but... the function works fine outside of testing.....wtf
#def test_create_events_from_matched_points():
#    FWA.create_events_from_matched_points("temp.test_points", "id",
#                                          "temp.test_events", 250)

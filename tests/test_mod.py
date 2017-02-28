from __future__ import absolute_import
import pandas as pd

import fwakit

FWA = fwakit.FWA()

# load test data to postgres
df = pd.read_csv(r'tests/data/test.csv')
df.to_sql('test_points', FWA.db.engine, if_exists='replace', schema=FWA.schema)


def test_initialize():
    assert FWA.bad_linear_features[0] == 110037498


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
    FWA.add_ltree(table=FWA.schema+'.test_points')
    assert 'wscode_ltree' in FWA.db[FWA.schema+'.test_points'].columns




# this works
#def test_st_distance():
#    r = FWA.db.query("""SELECT ST_Distance(ST_GeomFromText('POINT(0 0)'),
#                                           ST_GeomFromText('POINT(0 10)'))""")

# but this doesn't, psycopg2 wants ST_Distance inputs cast explicitly
# but... the function works fine outside of testing.....wtf
#def test_create_events_from_matched_points():
#    FWA.create_events_from_matched_points("temp.test_points", "id",
#                                          "temp.test_events", 250)

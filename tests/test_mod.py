from __future__ import absolute_import
import pandas as pd

import fwakit


fwa = fwakit.FWA()

# load test table to postgres
df = pd.read_csv("tests/data/test.csv")
df.to_sql("test_points", fwa.db.engine, if_exists="replace", schema="temp")


def test_initialize():
    assert fwa.bad_linear_features[0] == 110037498


def test_trim_ws_code():
    assert '900' == fwa.trim_ws_code('900-000000')
    assert '900-123456' == fwa.trim_ws_code('900-123456-000000')


def test_list_groups():
    groups = fwa.list_groups()
    assert groups[0] == 'ADMS'
    assert len(groups) == 246


def test_get_local_code():
    assert fwa.trim_ws_code(fwa.get_local_code(356532171, 4500)) == \
      "300-625474-020842-997520-317350-717769"


def test_add_ltree():
    fwa.add_ltree("temp.test_points")
    assert "wscode_ltree" in fwa.db["temp.test_points"].columns


# this works
#def test_st_distance():
#    r = fwa.db.query("""SELECT ST_Distance(ST_GeomFromText('POINT(0 0)'),
#                                           ST_GeomFromText('POINT(0 10)'))""")

# but this doesn't, psycopg2 wants ST_Distance inputs cast explicitly
# but... the function works fine outside of testing.....wtf
#def test_create_events_from_matched_points():
#    fwa.create_events_from_matched_points("temp.test_points", "id",
#                                          "temp.test_events", 250)

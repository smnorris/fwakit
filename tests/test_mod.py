from __future__ import absolute_import

import fwakit as fwa


#def test_trim_ws_code():
#    assert '920' == fwa.trim_ws_code('920-000000')
#    assert '920-123456' == fwa.trim_ws_code('920-123456-000000')


#def test_list_groups():
#    groups = fwa.list_groups(table='whse_basemapping.fwa_stream_networks_sp')
#    assert groups[0] == 'VICT'
#    assert len(groups) == 1


#def test_get_local_code():
#    assert (fwa.trim_ws_code(fwa.get_local_code(354155107, 3400)) ==
#            '920-076175-303123')


#def test_add_ltree():
#    table = 'whse_basemapping.fwa_stream_networks_sp'
#    db = fwa.util.connect()
#    if 'wscode_ltree' in db[table].columns:
#        db.execute("ALTER TABLE {t} DROP COLUMN wscode_ltree".format(t=table))
#    if 'localcode_ltree' in db[table].columns:
#        db.execute("ALTER TABLE {t} DROP COLUMN localcode_ltree".format(t=table))
#    fwa.add_ltree('whse_basemapping.fwa_stream_networks_sp')
#    assert 'wscode_ltree' in db['whse_basemapping.fwa_stream_networks_sp'].columns
#    assert 'localcode_ltree' in db['whse_basemapping.fwa_stream_networks_sp'].columns

# this works
#def test_st_distance():
#    r = FWA.db.query("""SELECT ST_Distance(ST_GeomFromText('POINT(0 0)'),
#                                           ST_GeomFromText('POINT(0 10)'))""")

# but this doesn't, psycopg2 wants ST_Distance inputs cast explicitly
# but... the function works fine outside of testing.....wtf
#def test_create_events_from_matched_points():
#    FWA.create_events_from_matched_points("temp.test_points", "id",
#                                          "temp.test_events", 250)

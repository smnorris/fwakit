from __future__ import absolute_import
import os

import fwakit as fwa


DB_URL = os.environ['FWA_DB_TEST']
TEST_SHP = r'tests/data/pscis.shp'


def test_setUp():
    db = fwa.util.connect(DB_URL)
    db.execute('CREATE SCHEMA IF NOT EXISTS whse_fish')
    db['whse_fish.pscis_events_1'].drop()
    db['whse_fish.pscis_events_2'].drop()
    db.ogr2pg(TEST_SHP, schema='whse_fish')
    assert 'whse_fish' in db.schemas
    assert 'whse_fish.pscis' in db.tables


def test_reference_points():
    db = fwa.util.connect(DB_URL)
    fwa.reference_points('whse_fish.pscis',
                         'pt_id',
                         'whse_fish.pscis_events_1',
                         300,
                         db=db)
    r = db.query('SELECT COUNT(*) FROM whse_fish.pscis_events_1')
    assert r.fetchone()[0] == 228


def test_get_closest_points():
    # more of a guide than a test
    db = fwa.util.connect(DB_URL)
    sql = """CREATE TABLE whse_fish.pscis_events_2 AS
             WITH closest AS (
               SELECT DISTINCT ON (pt_id)
                 pt_id,
                 distance_to_stream
               FROM whse_fish.pscis_events_1
               ORDER BY pt_id, distance_to_stream
             )
             SELECT DISTINCT e.*
             FROM whse_fish.pscis_events_1 e
             INNER JOIN closest ON e.pt_id = closest.pt_id
             AND e.distance_to_stream = closest.distance_to_stream
          """
    db.execute(sql)
    r = db.query('SELECT COUNT(*) FROM whse_fish.pscis_events_2')
    assert r.fetchone()[0] == 97


def test_fwa_lengthupstream():
    db = fwa.util.connect(DB_URL)
    sql = """WITH pts AS
             (SELECT blue_line_key, downstream_route_measure
             FROM whse_fish.pscis_events_2
             WHERE pt_id = 1712)
             SELECT fwa_lengthupstream(pts.blue_line_key,
                                       pts.downstream_route_measure)
             FROM pts"""
    r = db.query(sql)
    assert round(r.fetchone()[0]) == 1483

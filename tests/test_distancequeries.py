import os

import fwakit as fwa

DB_URL = os.environ['FWA_DB_TEST']
# test downstream distance at various locations

dnstr_query = "SELECT fwa_lengthdownstream(%s, %s)"
upstr_query = "SELECT fwa_lengthupstream(%s, %s)"
instr_query = "SELECT fwa_lengthinstream(%s, %s, %s, %s)"


def test_setup():
    db = fwa.util.connect(DB_URL)
    db.execute(fwa.queries['fwa_lengthdownstream'])
    db.execute(fwa.queries['fwa_lengthupstream'])
    db.execute(fwa.queries['fwa_lengthinstream'])


def test_dnstr_mouth():
    blkey = 354153694
    measure = 0
    db = fwa.util.connect(DB_URL)
    r = db.query(dnstr_query, (blkey, measure)).fetchone()
    assert r[0] == 0


def test_dnstr_straight_shot():
    blkey = 354153694
    measure = 2500
    db = fwa.util.connect(DB_URL)
    r = db.query(dnstr_query, (blkey, measure)).fetchone()
    assert r[0] == 2500


def test_dnstr_branching_1():
    blkey = 354132117
    measure = 1900
    db = fwa.util.connect(DB_URL)
    r = db.query(dnstr_query, (blkey, measure)).fetchone()
    assert round(r[0], 2) == 6313.67


def test_dnstr_branching_2():
    blkey = 354133856
    measure = 100
    db = fwa.util.connect(DB_URL)
    r = db.query(dnstr_query, (blkey, measure)).fetchone()
    assert round(r[0], 2) == 92881.36


def test_dnstr_sidechannel():
    blkey = 354088178
    measure = 20
    db = fwa.util.connect(DB_URL)
    r = db.query(dnstr_query, (blkey, measure)).fetchone()
    assert r[0] is None


def test_upstr_mouth():
    blkey = 354141556
    measure = 0
    db = fwa.util.connect(DB_URL)
    r = db.query(upstr_query, (blkey, measure)).fetchone()
    assert round(r[0], 2) == 3689.59


def test_upstr_equicodes():
    blkey = 354141556
    measure = 1400
    db = fwa.util.connect(DB_URL)
    r = db.query(upstr_query, (blkey, measure)).fetchone()
    assert round(r[0], 2) == 2289.59


def test_upstr_nonequicodes():
    blkey = 354148866
    measure = 2800
    db = fwa.util.connect(DB_URL)
    r = db.query(upstr_query, (blkey, measure)).fetchone()
    assert round(r[0], 2) == 19910.22


def test_instr_sameblueline():
    blkey = 354148866
    measure_a = 10
    measure_b = 1000
    db = fwa.util.connect(DB_URL)
    r = db.query(instr_query, (blkey, measure_a, blkey, measure_b)).fetchone()
    assert round(r[0], 2) == 990


def test_instr_diffblueline():
    blkey_a = 354148866
    measure_a = 10
    blkey_b = 354121034
    measure_b = 50
    db = fwa.util.connect(DB_URL)
    r = db.query(instr_query,
                 (blkey_a, measure_a, blkey_b, measure_b)).fetchone()
    assert round(r[0], 2) == 6109.81

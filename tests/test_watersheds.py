import fwakit as fwa
from fwakit import watersheds

test_points = [(1420528, 652194),  # Pinantan Lake: no refinement, check all wb polys present
               (1404246, 695549),  # to be refined by DEM
               (1403072, 686047),  # to be refined by cutting
               (1437569, 484291),  # to be refined by cutting
               (1397255, 508884),
               (1395904, 468799),
               (1494176, 638685),
               (1395817, 468862), # Pasayten (large USA contributing area)]
               (678173, 1679406), # Smart River, YT border
               (1365183, 582989)] # Nicola River, cut
               #(1435172, 475689)]  # Ewart Creek (small USA contrib area)
               #(1529332, 467655),  # Kettle, just outside of Midway (in USA)
               #(1848955, 503291)]  # Flathead (in USA)


def setup():
    db = fwa.util.connect()
    db['public.fwakit_point_test'].drop()
    db['public.fwakit_point_test_referenced'].drop()
    db.execute("""
               CREATE TABLE fwakit_point_test
               (id serial primary key, geom geometry (POINT, 3005))
               """
               )
    for point in test_points:
        db.execute("""
               INSERT INTO fwakit_point_test (geom)
               VALUES (ST_SetSRID(ST_MakePoint(%s, %s), 3005))
               """, (point[0], point[1]))
    fwa.reference_points(
        'public.fwakit_point_test',
        'id',
        'public.fwakit_point_test_referenced',
        125,
        closest=True,
        db=db)


def test_points_to_prelim_watersheds():
    db = fwa.util.connect()
    db['public.fwakit_prelimwsd_test'].drop()
    watersheds.points_to_prelim_watersheds(
        'public.fwakit_point_test_referenced',
        'id',
        'public.fwakit_prelimwsd_test',
        db=db,
        dissolve=False)
    assert 'public.fwakit_prelimwsd_test' in db.tables


def test_get_refine_method():
    db = fwa.util.connect()
    pt = db['public.fwakit_point_test_referenced'].find_one(id=1)
    refine_method = watersheds.get_refine_method(pt, db=db)
    assert refine_method == 'DROP'
    pt = db['public.fwakit_point_test_referenced'].find_one(id=2)
    refine_method = watersheds.get_refine_method(pt, db=db)
    assert refine_method == 'DEM'
    pt = db['public.fwakit_point_test_referenced'].find_one(id=3)
    refine_method = watersheds.get_refine_method(pt, db=db)
    assert refine_method == 'CUT'
    pt = db['public.fwakit_point_test_referenced'].find_one(id=4)
    refine_method = watersheds.get_refine_method(pt, db=db)
    assert refine_method == 'CUT'
    pt = db['public.fwakit_point_test_referenced'].find_one(id=5)
    refine_method = watersheds.get_refine_method(pt, db=db)
    assert refine_method == 'CUT'
    pt = db['public.fwakit_point_test_referenced'].find_one(id=6)
    refine_method = watersheds.get_refine_method(pt, db=db)
    assert refine_method == 'CUT'
    pt = db['public.fwakit_point_test_referenced'].find_one(id=7)
    refine_method = watersheds.get_refine_method(pt, db=db)
    assert refine_method == 'CUT'
    pt = db['public.fwakit_point_test_referenced'].find_one(id=8)
    refine_method = watersheds.get_refine_method(pt, db=db)
    assert refine_method == 'CUT'
    pt = db['public.fwakit_point_test_referenced'].find_one(id=10)
    refine_method = watersheds.get_refine_method(pt, db=db)
    assert refine_method == 'CUT'

# def test_add_local_watershed():
#     db = fwa.util.connect()
#     watersheds.add_local_watersheds(
#         'public.fwakit_point_test_referenced',
#         'id',
#         'public.fwakit_prelimwsd_test',
#         db=db)


#def teardown():
#    db = fwa.util.connect()
#    db['public.fwakit_point_test'].drop()
#    db['public.fwakit_point_test_referenced'].drop()
#    db['public.fwakit_prelimwsd_test'].drop()
#    db['public.fwakit_prelimwsd_exbc_test'].drop()

import fwakit

fwa = fwakit.FWA()


def test_initialize():
    assert fwa.bad_linear_features[0] == 110037498


def test_replace_query_vars():
    sql = "SELECT $myInputField FROM $myInputTable"
    lookup = {'myInputField': 'customer_id', 'myInputTable': 'customers'}
    sql = fwa.replace_query_vars(sql, lookup)
    assert sql == "SELECT customer_id FROM customers"


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

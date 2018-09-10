"""
Tools for working with BC Freshwater Atlas loaded to PostgreSQL
  - upstream / downstream query construction
  - conveniently connect to db
  - index tables, add ltree types for speeding of queries
  - methods for describing and interacting with event tables
"""

from __future__ import absolute_import

import datetime
import re

from sqlalchemy.dialects.postgresql import INTEGER, BIGINT

import fwakit as fwa
from fwakit import util


queries = util.QueryDict()


def list_groups(table=None, db=None):
    """Return sorted list of watershed groups in specified table
    """
    if not db:
        db = util.connect()
    if not table:
        table = fwa.tables["fwa_watershed_groups_poly"]
    groups = db[table].distinct("watershed_group_code")
    return sorted(groups)


def trim_ws_code(code):
    """Trim the trailing zeros from input watershed code/local code
    """
    return re.sub(r"(\-000000)+$", "", code)


def get_local_code(blue_line_key, measure, db=None):
    """Given a blue_line_key and measure, return local watershed code
    """
    if not db:
        db = util.connect()
    result = db.query_one(fwa.queries['get_local_code'],
                          (blue_line_key, measure))
    if result:
        return result[0]
    else:
        return None


def add_ltree(table, column_lookup={"fwa_watershed_code": "wscode_ltree",
                                    "local_watershed_code": "localcode_ltree"},
              db=None):
    """
    Add watershed code ltree types and indexes to specified table.
    (making a copy of the table is *much* faster than updating)
    """
    if not db:
        db = util.connect()
    # only add columns if source is present and new column not present
    new_columns = {k: v for (k, v) in column_lookup.items()
        if k in db[table].columns and v not in db[table].columns}
    if new_columns:
        # create new table
        db[table+"_tmp"].drop()
        db.execute("""CREATE TABLE {t}_tmp
                      (LIKE {t} INCLUDING ALL)""".format(t=table))
        ltree_list = []
        # add columns to new table
        for column in new_columns:
            db.execute("""ALTER TABLE {t}_tmp ADD COLUMN {c} ltree
                       """.format(t=table, c=column_lookup[column]))
            # add columns to select string
            ltree_list.append("fwa_wsc2ltree({incolumn}) as {outcolumn}"
                .format(incolumn=column, outcolumn=column_lookup[column]))

        # insert data
        ltree_sql = ", ".join(ltree_list)
        sql = """INSERT INTO {t}_tmp (SELECT *, {ltree_sql} FROM {t})
              """.format(ltree_sql=ltree_sql, t=table)
        db.execute(sql)

        # drop original table
        db[table].drop()
        # rename new table back to original name
        _, tablename = db.parse_table_name(table)
        db[table+'_tmp'].rename(tablename)

        # create ltree indexes
        for column in new_columns.values():
            for index_type in ["btree", "gist"]:
                db[table].create_index([column], index_type=index_type)


def get_events(table, pk, filters=None, param=None, db=None):
    """
    Return blue line key event info from supplied event table

    table      - table to query
    pk         - event id
    filters    - list of sql filter strings
                 for example:
                 ["watershed_group_code = 'LARL'",
                  "fish_habitat is not null",
                  "(wscode_ltree ~ %s OR wscode_ltree ~ %s"]
    param      - parameters to supply to the query
                 (replacing %s in the filters)
    """
    if not db:
        db = util.connect()
    sql = """SELECT {pk},
                blue_line_key,
                downstream_route_measure,
                fwa_watershed_code,
                local_watershed_code
             FROM {table}""".format(pk=pk,
                                    table=table)
    if filters:
        sql = sql + "\nWHERE " + "\n AND ".join(filters)
    sql = sql + "\nORDER BY {pk}".format(pk=pk)
    if param:
        return db.query(sql, param)
    else:
        return db.query(sql)


def reference_points(point_table, point_id, out_table, threshold=100, closest=False,
                     db=None):
    """Create a table that references input points to stream network
    """
    if not db:
        db = util.connect()
    # create preliminary table, with all potential matches within threshold
    sql = db.build_query(
        fwa.queries['reference_points'],
        {'point_table': point_table,
         'point_id': point_id,
         'out_table': out_table})
    db.execute(sql, (threshold))
    # if 'closest' is specified, only retain the closest match
    if closest:
        db[out_table + "_t"].drop()
        sql = """
           CREATE TABLE {out_table}_t AS
           SELECT DISTINCT ON ({point_id}) *
           FROM {out_table}
           ORDER BY {point_id}, distance_to_stream
           """.format(out_table=out_table,
                      point_id=point_id)
        db.execute(sql)
        db[out_table].drop()
        schema, table = db.parse_table_name(out_table)
        db.execute("""ALTER TABLE {out_table}_t RENAME TO {table}
                   """.format(out_table=out_table,
                              table=table))
    return out_table


def create_geom_from_events(in_table,
                            out_table,
                            geom_type=None,
                            db=None):
    '''
    Copy input event table, adding and populating a geometry field
    corresponding to the event measures

    in_table    - input event table
    out_table   - output table to copy to
    geom_type   - POINT|LINE

    note that point output untested.
    '''
    # overwrite the table if it already exists
    if not db:
        db = util.connect()
    db[out_table].drop()
    # if not specified, determine if events are point or line
    # line events have length_metre
    for req_column in ["blue_line_key", "downstream_route_measure"]:
        if req_column not in db[in_table].columns:
            raise ValueError("Column {c} does not exist".format(c=req_column))
    if not geom_type:
        if "length_metre" in db[in_table].columns:
            geom_type = "LINE"
        else:
            geom_type = "POINT"
    if geom_type == "POINT":
        sql = fwa.queries["events_to_points"]
    if geom_type == 'LINE':
        sql = fwa.queries["events_to_lines"]
    if geom_type not in ["POINT", "LINE"]:
        raise ValueError('create_geom_from_events: geomType must be POINT or LINE')
    # modify query string with input/output table names
    query = db.build_query(sql, {"outputTable": out_table,
                                 "inputTable": in_table})
    db.execute(query)
    return True

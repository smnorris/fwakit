"""
Tools for working with BC Freshwater Atlas loaded to PostgreSQL
  - upstream / downstream query construction
  - conveniently connect to db
  - index tables, add ltree types for speeding of queries
  - methods for describing and interacting with event tables
"""

from __future__ import absolute_import

import datetime
import os
import pkg_resources
import re

from sqlalchemy.dialects.postgresql import INTEGER, BIGINT

import fwakit as fwa
from fwakit import util


class QueryDict(object):
    def __init__(self):
        self.queries = None

    def __getitem__(self, query_name):
        if pkg_resources.resource_exists(__name__, os.path.join("sql", query_name+'.sql')):
            return pkg_resources.resource_string(
                __name__,
                os.path.join("sql", query_name+'.sql')).decode('utf-8')

        else:
            raise ValueError("Invalid query name: %r" % query_name)

queries = QueryDict()


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

    Making a copy of the table is *much* faster than adding columns and updating
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


def create_events_from_points(point_table, point_id, out_table,
                              threshold, match_col=None, db=None):
    """
      Locate points in provided input table on stream network.

      Outputs a table with following fields:
          point_id (input points' integer unique id)
          linear_feature_id
          blue_line_key
          downstream_route_measure
          fwa_watershed_code
          local_watershed_code
          watershed_group_code
          waterbody_key
          distance_to_stream (distance from point to matched stream)
          match number (1, 2, 3 etc in order of distance)

      pointTable - name of source point table
      pointId    - INTEGER field holding unique id within pointTable
      outTable   - name of destination event table within db
                   note - overwritten if it already exists
      threshold  - max distance of source points from stream (doesn't apply
                   for lakes)

      NOTES
      - All inputs and outputs must be in the same database
    """
    if not db:
        db = util.connect()
    # check that match_col is present if specified
    if match_col and match_col not in db[point_table].columns:
        raise ValueError('match column not in source table')
    # check that id is an integer field
    if type(db[point_table].column_types[point_id]) not in (INTEGER,
                                                                 BIGINT):
        raise ValueError('source table pk must be integer/bigint')
    # grab id and fwa_watershed_code of all points
    if match_col:
        pts = list(db[point_table].distinct(point_id,
                                                 match_col))
    else:
        pts = list(db[point_table].distinct(point_id))
    # Find nearest neighbouring stream
    event_list = []
    start_time = datetime.datetime.utcnow()
    for n, pt in enumerate(pts):
        if match_col:
            pt = pt[point_id]
        #self.note_progress('create_events_from_points: ', n,len(pts), start_time)
        matched = False
        # get streams
        sql = db.build_query(fwa.queries['streams_closest_to_point'],
                                  {"inputPointTable": point_table,
                                   "inputPointId": point_id})
        # get ws_code and distance to stream of nearby distinct streams
        streams = db.query(sql, (pt, threshold))
        if streams:
        # if the point's match field is the same as one of the stream's,
        # use only that stream
            if match_col:
                if pt[match_col] in [r[match_col] for r in streams]:
                    # move matching stream to front of the list
                    streams = [s for s in streams
                               if s[match_col] == pt[match_col]]
                    matched = True
            #n_other_nearby_streams = len(streams) - 1
            #stream = streams[0]
            for n_stream, stream in enumerate(streams):
                stream_code = stream["fwa_watershed_code"]
                distance_to_stream = stream["distance_to_stream"]
                # snap the point to the matching stream
                sql = db.build_query(fwa.queries['locate_point_on_stream'],
                                          {"inputPointTable": point_table,
                                           "inputPointId": point_id})
                row = db.query_one(sql, (pt, stream_code, threshold))
                # append row to list
                if row["downstream_route_measure"]:
                    insert_row = dict(row)
                    insert_row[point_id] = pt
                    insert_row["matched"] = matched
                    insert_row["distance_to_stream"] = distance_to_stream
                    #insert_row["n_other_nearby_streams"] = n_other_nearby_streams
                    insert_row["n_stream_match"] = n_stream + 1
                    event_list.append(insert_row)
    if event_list:
        db[out_table].drop()
        sql = """CREATE TABLE {out_table}
                   (
                   {point_id} int,
                   linear_feature_id int8,
                   blue_line_key int4,
                   downstream_route_measure float8,
                   fwa_watershed_code text,
                   local_watershed_code text,
                   waterbody_key int4,
                   watershed_group_code text,
                   distance_to_stream float8,
                   matched boolean,
                   n_stream_match int
                   )""".format(out_table=out_table,
                               point_id=point_id)
                  # --n_other_nearby_streams int
        db.execute(sql)
        db[out_table].insert_many(event_list)
    else:
        return 'No input points within %sm of a stream.' % str(threshold)


def create_geom_from_events(self,
                            in_table,
                            out_table,
                            geom_type=None):
    '''
    Copy input event table, adding and populating a geometry field
    corresponding to the event measures

    in_table    - input event table
    out_table   - output table to copy to
    geom_type   - POINT|LINE

    note that point output untested.
    '''
    # overwrite the table if it already exists
    self.db[out_table].drop()
    # if not specified, determine if events are point or line
    # line events have length_metre
    for req_column in ["blue_line_key", "downstream_route_measure"]:
        if req_column not in self.db[in_table].columns:
            raise ValueError("Column {c} does not exist".format(c=req_column))
    if not geom_type:
        if "length_metre" in self.db[in_table].columns:
            geom_type = "LINE"
        else:
            geom_type = "POINT"
    if geom_type == "POINT":
        sql = self.queries["events_to_points"]
    if geom_type == 'LINE':
        sql = self.queries["events_to_lines"]
    if geom_type not in ["POINT", "LINE"]:
        raise ValueError('create_geom_from_events: geomType must be POINT or LINE')
    # modify query string with input/output table names
    query = self.db.build_query(sql, {"outputTable": out_table,
                                      "inputTable": in_table})
    self.db.execute(query)
    return True

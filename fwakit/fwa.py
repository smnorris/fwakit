"""
Tools for working with BC Freshwater Atlas
  - upstream / downstream query construction
  - conveniently connect to db
  - index tables, add ltree types for speeding of queries
  - methods for describing and interacting with event tables
"""

import os
import pkg_resources
import json
import re
import datetime

import sqlalchemy
from sqlalchemy.dialects.postgresql import INTEGER, BIGINT

import pgdb


PROCESS_LOG_INTERVAL = 100


class FWA(object):
    """
    Hold connection to FWA database
    """
    def __init__(self, db=None, connect=True):
        # load the db parameters
        db_param = json.loads(pkg_resources.resource_string(__name__,
                              "param/database.json"))
        # if no connection is provided, create one
        if not db:
            self.dburl = db_param["dburl"]
            self.schema = db_param["schema"]
            if connect:
                self.db = pgdb.connect(self.dburl)
        # if a connection is provided, use it
        else:
            self.db = db
            self.dburl = self.db.url
            # if schema is specified in provided connection, use it
            if db.schema:
                self.schema = db.schema
            # if no schema is specified, default to the schema in db params
            else:
                self.schema = db_param["schema"]
        # load data source parameters
        self.source = json.loads(pkg_resources.resource_string(__name__,
                                 "param/source.json"))
        # define shortcuts to tables
        self.tables = {}
        for layer in self.source["layers"]:
            self.tables[layer["alias"]] = self.schema+"."+layer["table"]

        self.log_interval = PROCESS_LOG_INTERVAL
        # note bad stream data
        # these are linear_feature_ids for lines where watershed/local codes
        # are non-standard, not permitting up/down stream queries
        self.bad_linear_features = [110037498, 110037870, 110037183, 110037869,
                                    110037877, 110037352, 110037541, 110037533,
                                    110037537, 110037659, 832599689, 832631053,
                                    831802658, 831896651, 707558014, 213037736,
                                    213037642, 213037651, 700335558, 700335564]
        # exclude entire blue lines
        self.bad_blue_lines = [356349577]
        # load all queries in the sql folder
        self.queries = {}
        for f in pkg_resources.resource_listdir(__name__, "sql"):
            key = os.path.splitext(f)[0]
            self.queries[key] = pkg_resources.resource_string(__name__,
                                                            os.path.join("sql",
                                                                         f))

    def note_progress(self, function, idx, total, start_time):
        '''
        Note progress of a function
        '''
        if idx % self.log_interval == 0 and idx != 0:
            end_time = datetime.datetime.utcnow()
            elapsed = end_time - start_time
            outStr = """%s - completed %s of %s records in %s ...""" % (function,
                                                                       str(idx),
                                                                       str(total),
                                                                       str(elapsed))
            print outStr
            self.startTime = datetime.datetime.utcnow()

    def list_groups(self, table=None):
        """
        Return sorted list of watershed groups in specified table
        """
        if not table:
            table = self.tables["groups"]
        groups = self.db[table].distinct("watershed_group_code")
        return sorted(groups)

    def trim_ws_code(self, code):
        '''
        Trim the trailing zeros from input watershed code/local code
        '''
        return re.sub(r"(\-000000)+$", "", code)

    def get_local_code(self, blue_line_key, measure):
        """
        Given a blue_line_key and measure, return local watershed code
        """
        result = self.db.query_one(self.queries["get_local_code"],
                                   (blue_line_key, measure))
        if result:
            return result[0]
        else:
            return None

    def add_ltree(self, table, column_lookup={"fwa_watershed_code":
                                              "wscode_ltree",
                                              "local_watershed_code":
                                              "localcode_ltree"}):
        """ Add watershed code ltree types and indexes to specified table
            To speed things up this makes a copy of table, any existing
            indexes/constraints will have to be regenerated

            SQL is generated programatically (rather than living in /sql) so
            we can handle tables where local_watershed_code does not exist.
        """
        schema, tablename = self.db.parse_table_name(table)
        # create new table
        temptable = schema+"."+"temp_ltree_copy"
        self.db[temptable].drop()
        ltree_list = []
        for column in column_lookup:
            if column in self.db[table].columns:
                ltree_list.append(
                    """CASE WHEN POSITION('-' IN wscode_trim({incolumn})) > 0
                           THEN text2ltree(REPLACE(wscode_trim({incolumn}),'-','.'))
                           ELSE  text2ltree(wscode_trim({incolumn}))
                       END as {outcolumn}""".format(incolumn=column,
                                                    outcolumn=column_lookup[column]))
        ltree_sql = ", \n".join(ltree_list)
        sql = """CREATE TABLE {temptable} AS
                  SELECT
                    *,
                    {ltree_sql}
                  FROM {table}""".format(temptable=temptable,
                                         ltree_sql=ltree_sql,
                                         table=table)
        self.db.execute(sql)
        # drop original table
        self.db[table].drop()
        # rename new table back to original name
        self.db[temptable].rename(tablename)
        # create indexes
        for column in column_lookup:
            for index_type in ["btree", "gist"]:
                self.db[table].create_index(column_lookup[column],
                                            index_type=index_type)

    def get_events(self, table, pk, filters=None, param=None):
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
            return self.db.query(sql, param)
        else:
            return self.db.query(sql)

    def create_events_from_matched_points(self, point_table, point_id,
                                          out_table, threshold):
        """
          Locate points in provided input table on stream network.

          Returns multiple matches - matches to all distinct streams within
          specified tolerance unless there is a fwa_watershed_code match, in
          which case only the event on matching stream is returned.

          Outputs a table with following fields:
              event_id (unique identifier)
              pointId (input points' integer unique id)
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
          - input table must include column fwa_watershed_code
        """
        # check that watershed code is present
        if "fwa_watershed_code" not in self.db[point_table].columns:
            raise ValueError('column fwa_watershed_code not in source table')
        # check that point id is an integer field
        if type(self.db[point_table].column_types[point_id]) not in (INTEGER,
                                                                     BIGINT):
            raise ValueError('source table pk must be integer/bigint')
        # grab id and fwa_watershed_code of all points
        pts = list(self.db[point_table].distinct(point_id,
                                                 'fwa_watershed_code'))
        # Find nearest neighbouring stream
        event_list = []
        start_time = datetime.datetime.utcnow()
        for n, pt in enumerate(pts):
            self.note_progress('create_events_from_matched_points: ', n,
                               len(pts), start_time)
            # get streams
            sql = self.db.build_query(self.queries['streams_closest_to_point'],
                                      {"inputPointTable": point_table,
                                       "inputPointId": point_id})
            # get ws_code of distinct streams within set distance
            streams = self.db.query(sql, (pt["id"], pt["id"], threshold))
            stream_codes = [r[0] for r in streams]
            # if the point's watershed code is the same as one of the stream's,
            # use only that stream
            if pt["fwa_watershed_code"] in stream_codes:
                streams = [s for s in streams
                           if s["fwa_watershed_code"] ==
                           pt["fwa_watershed_code"]]
            # loop through nearby streams (or just the one if code matched)
            counter = 1
            for stream in streams:
                stream_code = stream["fwa_watershed_code"]
                distance_to_stream = stream["distance_to_stream"]
                # snap the point to the matching stream
                sql = self.db.build_query(self.queries['locate_point_on_stream'],
                                          {"inputPointTable": point_table,
                                           "inputPointId": point_id})
                row = self.db.query_one(sql, (pt["id"],
                                              stream_code, threshold))
                # append row to list
                if row["downstream_route_measure"]:
                    insert_row = dict(row)
                    insert_row["id"] = pt["id"]
                    insert_row["match_number"] = counter
                    insert_row["distance_to_stream"] = distance_to_stream
                    event_list.append(insert_row)
                    counter += 1
        if event_list:
            self.db[out_table].drop()
            sql = """CREATE TABLE {out_table}
                       (
                       event_id serial,
                       {point_id} int,
                       linear_feature_id int8,
                       blue_line_key int4,
                       downstream_route_measure float8,
                       fwa_watershed_code text,
                       local_watershed_code text,
                       waterbody_key int4,
                       watershed_group_code text,
                       distance_to_stream float8,
                       match_number int4
                       )""".format(out_table=out_table,
                                   point_id=point_id)
            self.db.execute(sql)
            self.db[out_table].insert_many(event_list)
        else:
            return 'No input points within %sm of a stream.' % str(threshold)

    def add_fishhab(self, table, pk, fish_habitat_table):
        """
        Add field `fish_habitat` to provided event table,
        Then populate by examining the fish habitat line events
        Overwrites existing fish_habitat field if it already exists

        Maybe speed up by doing a join rather than iterating through recs:
            SELECT
              e.id,
              h.blue_line_key,
              max(h.downstream_route_measure) as downstream_route_measure
            FROM temp.nfc_events e
            INNER JOIN fish_passage.fish_habitat h
            ON e.blue_line_key = h.blue_line_key
            AND e.downstream_route_measure >= h.downstream_route_measure
            GROUP BY e.id, h.blue_line_key
            ORDER BY e.id
        """
        # add fish habitat field, dropping if it already exists
        self.db[table].drop_column("fish_habitat")
        self.db[table].create_column("fish_habitat", sqlalchemy.Text)

        # select all events
        events = self.get_events(table, pk)
        start_time = datetime.datetime.utcnow()
        for n, event in enumerate(events, start=1):
            self.note_progress('add_fish_habitat: ', n,
                               len(events), start_time)
            # Get the fish_habitat value for the crossing from the line in the
            # fish habitat event table on which the crossing lies,
            lookup = {"InputTable": table,
                      "PrimaryKey": pk,
                      "FishHabitatTable": fish_habitat_table}
            sql = self.db.build_query(self.queries["add_habitat"], lookup)
            self.db.execute(sql, (event["blue_line_key"],
                                  event["downstream_route_measure"],
                                  event[pk]))

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

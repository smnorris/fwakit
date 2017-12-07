"""
Tools for working with BC Freshwater Atlas loaded to PostgreSQL
  - upstream / downstream query construction
  - conveniently connect to db
  - index tables, add ltree types for speeding of queries
  - methods for describing and interacting with event tables
"""

from __future__ import absolute_import
from __future__ import print_function
import os
import pkg_resources
import re
import datetime

import yaml
import sqlalchemy
from sqlalchemy.dialects.postgresql import INTEGER, BIGINT

import pgdb
from . import config

PROCESS_LOG_INTERVAL = 100


class FWA(object):
    """Hold connection to FWA database
    """
    def __init__(self, config=config.config, db=None, connect=True):
        # load config - accept either a path or a dict
        if isinstance(config, dict):
            self.config = config
        elif isinstance(config, str):
            with open(config) as config_file:
                self.config = yaml.load(config_file)
        else:
            raise ValueError('Config must be a string or a dict')

        # if no connection is provided, create one
        if not db:
            self.dburl = self.config["db_url"]
            self.schema = self.config["db_schema"]
            if connect:
                self.db = pgdb.connect(self.dburl)
        # if a connection is provided, use it
        else:
            self.db = db
            self.dburl = self.db.url
            # if schema is specified in provided connection, use it
            if db.schema:
                self.schema = db.schema
            # if no schema is specified, use schema in config
            else:
                self.schema = self.config["db_schema"]
        # define shortcuts to tables
        self.tables = {}
        for source_file in self.config["source_files"]:
            for table in self.config["source_files"][source_file]:
                t = self.config["source_files"][source_file][table]
                self.tables[table] = self.schema + "." + table
                # add the configured table def to config attributes
                self.config[table] = t

        self.log_interval = PROCESS_LOG_INTERVAL
        # load all queries in the sql folder
        self.queries = {}
        for f in pkg_resources.resource_listdir(__name__, "sql"):
            key = os.path.splitext(f)[0]
            self.queries[key] = pkg_resources.resource_string(__name__,
                                                            os.path.join("sql",
                                                                         f))
        # There are a handful of linear features where watershed codes are invalid
        # for up/down stream queries, make sure they aren't used
        self.tables['fwa_invalid_codes'] = self.schema+'.fwa_invalid_codes'
        if self.tables['fwa_stream_networks_sp'] in self.db.tables:
            if self.tables['fwa_invalid_codes'] not in self.db.tables:
                lookup = {'InTable': self.tables['fwa_stream_networks_sp'],
                          'OutTable': self.tables['fwa_invalid_codes']}
                self.db.execute(self.db.build_query(
                                self.queries["invalid_codes"], lookup))
            self.invalid_streams = (self.db[self.tables['fwa_invalid_codes']]
                                        .distinct('linear_feature_id'))

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
            print(outStr)
            self.startTime = datetime.datetime.utcnow()

    def list_groups(self, table=None):
        """Return sorted list of watershed groups in specified table
        """
        if not table:
            table = self.tables["fwa_watershed_groups_poly"]
        groups = self.db[table].distinct("watershed_group_code")
        return sorted(groups)

    def trim_ws_code(self, code):
        """Trim the trailing zeros from input watershed code/local code
        """
        return re.sub(r"(\-000000)+$", "", code)

    def get_local_code(self, blue_line_key, measure):
        """Given a blue_line_key and measure, return local watershed code
        """
        result = self.db.query_one(self.queries["get_local_code"],
                                   (blue_line_key, measure))
        if result:
            return result[0]
        else:
            return None

    def add_ltree(self, table, column_lookup={"fwa_watershed_code": "wscode_ltree",
                                              "local_watershed_code": "localcode_ltree"}):
        """
        Add watershed code ltree types and indexes to specified table
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
        # only add columns if not present
        new_columns = [c for c in column_lookup.values() if c not in self.db[table].columns]
        for column in column_lookup:
            if column in self.db[table].columns and column_lookup[column] in new_columns:
                ltree_list.append(
                    """CASE WHEN POSITION('-' IN wscode_trim({incolumn})) > 0
                              THEN text2ltree(REPLACE(wscode_trim({incolumn}), '-', '.'))
                            WHEN {incolumn} IS NULL THEN NULL
                            ELSE text2ltree(wscode_trim({incolumn}))
                       END as {outcolumn}""".format(incolumn=column,
                                                    outcolumn=column_lookup[column]))
        ltree_sql = ", ".join(ltree_list)
        sql = """CREATE TABLE {temptable} AS
                 SELECT *, {ltree_sql}
                 FROM {table}
              """.format(temptable=temptable, ltree_sql=ltree_sql, table=table)
        self.db.execute(sql)

        # drop original table
        self.db[table].drop()
        # rename new table back to original name
        self.db[temptable].rename(tablename)
        # create ltree indexes
        for column in column_lookup:
            if column in self.db[table].columns:
                for index_type in ["btree", "gist"]:
                    self.db[table].create_index([column_lookup[column]],
                                                index_type=index_type)

    def create_lut_50k_20k_wsc(self, table='fwa_streams_20k_50k_wsc'):
        """
        Create a simplified lookup relating 50k codes to 20k codes, the source
        lookup table relates linear_feature_id to 50k codes
        """
        self.db[self.schema+"."+table].drop()
        sql = """CREATE TABLE {schema}.{table} AS
                 SELECT DISTINCT
                   watershed_code_50k,
                   fwa_watershed_code_20k,
                   watershed_group_code_20k
                 FROM {schema}.fwa_streams_20k_50k
                 ORDER BY watershed_code_50k, fwa_watershed_code_20k
              """.format(schema=self.schema,
                         table=table)
        self.db.execute(sql)
        self.db[self.schema+"."+table].create_index(['watershed_code_50k'])
        self.db[self.schema+"."+table].create_index(['fwa_watershed_code_20k'])

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

    def create_events_from_points(self, point_table, point_id, out_table,
                                  threshold, match_col=None):
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
        # check that match_col is present if specified
        if match_col and match_col not in self.db[point_table].columns:
            raise ValueError('match column not in source table')
        # check that id is an integer field
        if type(self.db[point_table].column_types[point_id]) not in (INTEGER,
                                                                     BIGINT):
            raise ValueError('source table pk must be integer/bigint')
        # grab id and fwa_watershed_code of all points
        if match_col:
            pts = list(self.db[point_table].distinct(point_id,
                                                     match_col))
        else:
            pts = list(self.db[point_table].distinct(point_id))
        # Find nearest neighbouring stream
        event_list = []
        start_time = datetime.datetime.utcnow()
        for n, pt in enumerate(pts):
            if match_col:
                pt = pt[point_id]
            self.note_progress('create_events_from_points: ', n,
                               len(pts), start_time)
            matched = False
            # get streams
            sql = self.db.build_query(self.queries['streams_closest_to_point'],
                                      {"inputPointTable": point_table,
                                       "inputPointId": point_id})
            # get ws_code and distance to stream of nearby distinct streams
            streams = self.db.query(sql, (pt, threshold))
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
                    sql = self.db.build_query(self.queries['locate_point_on_stream'],
                                              {"inputPointTable": point_table,
                                               "inputPointId": point_id})
                    row = self.db.query_one(sql, (pt, stream_code, threshold))
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
            self.db[out_table].drop()
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

    def index_events_downstream(self, table, pk):
        '''
        Given a table with blueLineKey events, create a field which holds
        id of next event downstream from a given event.

         - overwrites downstream_event_id if column already exists
         - events cannot be on 999-999999 streams
         - required fields:
             blue_line_key,
             downstream_route_measure,
             fwa_watershed_code,
             local_watershed_code

        table - input event table
        pk    - name of unique identifier within event table, must be of
                     type integer
        '''
        # add index field
        dnstr_id = "downstream_event_id"
        self.db[table].drop_column(dnstr_id)
        self.db[table].create_column(table, INTEGER)
        events = self.get_events(table, pk)
        start_time = datetime.datetime.utcnow()
        for n, event in enumerate(events, start=1):
            self.note_progress('index_event_table_downstream', n, events.rowcount, start_time)
            #downstreamSql = self.sql_downstream_below_blueLineKey(wsCode, localCode)
            downstreamSql = self.sql_downstream_below_blueLineKey_non_recursive(event["fwa_watershed_code"],
                                                                                event["local_watershed_code"])
            # create subquery to inject into update statement
            if downstreamSql:
                downstreamSql = """UNION ALL
                            SELECT %s, fwa_watershed_code, local_watershed_code, downstream_route_measure
                            FROM %s WHERE %s
                         ORDER BY fwa_watershed_code DESC, downstream_route_measure DESC
                         """ % (event[pk],
                                table,
                                downstreamSql)
            else:
                downstreamSql = ""
            # create update statement
            updateSql = """UPDATE %s
                            SET downstream_event_id =
                                (SELECT %s
                                 FROM
                                 (SELECT %s, fwa_watershed_code, local_watershed_code, downstream_route_measure
                                    FROM %s
                                   WHERE blue_line_key = %s
                                     AND downstream_route_measure <  (%s - .00001)
                                  %s) as foo
                                 LIMIT 1)
                                 WHERE %s = '%s'""" % (eventTable, eventId,
                                                     eventId, eventTable,
                                                     str(blueLineKey), str(measure),
                                                     downstreamSql, eventId, str(idValue))
            self.db.execute(updateSql)

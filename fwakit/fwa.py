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

import pgdb

PROCESS_LOG_INTERVAL = 1000


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
            self.queries[f] = pkg_resources.resource_string(__name__,
                                                            os.path.join("sql",
                                                                         f))

    def add_schema_qualifier(self, table):
        if "." not in table:
            return self.schema+"."+table
        else:
            return table

    def replace_query_vars(self, sql, lookup):
        """
        Modify table and field name variables in a sql string with a dict.

        USAGE
        sql = 'SELECT $myInputField FROM $myInputTable'
        lookup = {'myInputField':'customer_id', 'myInputTable':'customers'}
        sql = fwa.replace_query_vars(sql, lookup)

        """
        for key, val in lookup.iteritems():
            sql = sql.replace('$'+key, val)
        return sql

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
        sql = """SELECT local_watershed_code
                 FROM {table}
                 WHERE blue_line_key = %s
                 AND downstream_route_measure - .0001 <= %s
                 ORDER BY downstream_route_measure desc
                 LIMIT 1
              """.format(table=self.tables["streams"])
        localCode = self.db.query(sql, (blue_line_key, measure))
        if localCode:
            return localCode[0][0]
        else:
            return None

    def add_ltree(self, table):
        """ Add watershed code ltree types and indexes to specified table
            To speed things up this makes a copy of table, any existing
            indexes/constraints will have to be regenerated
        """
        schema, table = self.db.parse_table_name(table)
        sql = self.queries["add_ltree.sql"]
        lookup = {"schema": schema,
                  "sourceTable": table}
        sql = self.replace_query_vars(sql, lookup)
        self.db.execute(sql)

    def downstream_sql(self, wscode, localcode):
        '''
        Build a sql expression to return records with watershed code downstream
        of given codes, BUT not with the same watershed code (not on the same
        blue line route)

        This could be simplified to just use WSCode as input, but works as is.

        Note that the query must operate on tables that don't have 999-999999
        records
        '''
        # translate the watershed codes into ltree paths
        wscode = wscode[:wscode.find('-000000-')].replace('-', '.')
        localcode = localcode[:localcode.find('-000000-')].replace('-', '.')
        n_segments = wscode.count('.')
        # Don't attempt to process streams running to ocean
        if wscode.count('.') > 0:
            for i in range(n_segments):
                if i == 0:
                    sql = """(wscode_ltree = '{wscode}'
                             AND subltree(localcode_ltree||text2ltree('000000'),1,2) < '{localcode}')
                          """.format(wscode=wscode[:3],
                                     localcode=localcode[4:10])
                else:
                    position = (i * 7) - 3
                    or_string = """
                         OR ((wscode_ltree = '{wscode}'
                         AND subltree(localcode_ltree||text2ltree('000000'),{p1},{p2}) < '{localcode}'))
                         """.format(wscode=wscode[:position + 6],
                                    p1=i+1,
                                    p2=i+2,
                                    localcode=localcode[position + 7:position + 13])
                    sql = sql + or_string
            return sql
        else:    # return nothing if stream runs to ocean.
            return None

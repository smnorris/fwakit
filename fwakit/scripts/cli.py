# Command line interface for loading freshwater atlas data
import os
import urlparse
import subprocess

import click

import pgdb
import fwakit
from fwakit import util


@click.group()
def cli():
    pass


@click.command()
def createdb():
    """Create local FWA database"""
    # load data definitions
    fwa = fwakit.FWA(connect=False)
    dburl = fwa.dburl
    u = urlparse.urlparse(dburl)
    dbname = u.path[1:]
    user = u.username
    userdb = urlparse.urlunparse((u.scheme, u.netloc, user, None, None, None))
    db = pgdb.connect(userdb)
    # setup the database/extensions/schema
    db.execute("CREATE DATABASE {db}".format(db=dbname))


@click.command()
def get_data():
    """Download FWA data

    Does not include linear boundaries, watershed boundaries and watershed
    polys, add these if required

    Tested only on OSX. Not going to work on windows, download manually.
    """
    fwa = fwakit.FWA()
    click.echo('Downloading freshwater atlas source data')
    # get data source
    cmd = """wget --trust-server-names -qNP /tmp {f}
          """.format(f=fwa.source["url"])
    subprocess.call(cmd, shell=True)
    # unzip the gdb
    cmd = """unzip -qun -d /tmp /tmp/{f}""".format(f=fwa.source["file"])
    subprocess.call(cmd, shell=True)


@click.command()
def load_data():
    """load source data to postgres"""
    fwa = fwakit.FWA()
    # create required extenstions/functions/schema if they don't exist
    fwa.db.execute("CREATE EXTENSION IF NOT EXISTS POSTGIS")
    fwa.db.execute("CREATE EXTENSION IF NOT EXISTS LTREE")
    fwa.db.execute("CREATE SCHEMA IF NOT EXISTS {s}".format(s=fwa.schema))
    fwa.db.execute(fwa.queries["functions.sql"])
    # load source data
    source_file = os.path.join("/tmp", fwa.source["file"])
    click.echo('Loading FWA source data to PostgreSQL database')
    for layer in fwa.source["layers"]:
        click.echo('Loading '+layer['alias']+' to '+layer['table'])
        # load layer
        util.gdb2pg(source_file,
                    layer['table'].upper(),
                    fwa.dburl,
                    fwa.schema)
        # drop ogr and esri columns
        table = fwa.schema+"."+layer['table']
        for column in ["ogc_fid", "geometry_area", "geometry_length"]:
            if column in fwa.db[table].columns:
                fwa.db[table].drop_column(column)
        # ensure _id primary/foreign keys are int - ogr maps them to floats
        # integer should be fine for all but linear_feature_id
        for column in fwa.db[table].columns:
            if column[-3:] == '_id':
                if column == "linear_feature_id":
                    column_type = 'bigint'
                else:
                    column_type = 'integer'
                sql = """ALTER TABLE {t} ALTER COLUMN {col} TYPE {type}
                      """.format(t=table, col=column, type=column_type)
                fwa.db.execute(sql)
        # add ltree columns to tables with watershed codes
        if 'fwa_watershed_code' in fwa.db[table].columns:
            click.echo('Adding ltree types and indexes')
            fwa.add_ltree(table)
        # add primary key constraint
        fwa.db[table].add_primary_key(layer["id"])
        # create indexes on columns noted in parameters
        for column in layer["fields"]:
            tablename = layer['table']
            fwa.db[table].create_index([column], tablename+"_"+column+"_idx")


cli.add_command(createdb)
cli.add_command(get_data)
cli.add_command(load_data)

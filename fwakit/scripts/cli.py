# Command line interface for loading freshwater atlas data
from __future__ import absolute_import
import urlparse
import tempfile
import urllib2
import zipfile

import click

import pgdb
import fwakit
from fwakit import util


@click.group()
def cli():
    pass


@click.command()
def createdb():
    """Create local FWA database
    """
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
    """Download FWA_BC.gdb from GeoBC
    """
    fwa = fwakit.FWA()
    click.echo('Downloading freshwater atlas source data')

    # download from ftp
    fp = tempfile.NamedTemporaryFile('wb',
                                     dir=tempfile.gettempdir(),
                                     suffix=".zip",
                                     delete=False)
    download = urllib2.urlopen(fwa.source["url"])
    file_size_dl = 0
    block_sz = 8192
    while True:
        buffer = download.read(block_sz)
        if not buffer:
            break
        file_size_dl += len(buffer)
        fp.write(buffer)
    fp.close()

    # unzip the gdb
    zipped_file = zipfile.ZipFile(fp.name, 'r')
    zipped_file.extractall()
    zipped_file.close()


@click.command()
@click.option('--layers', '-l', help="Comma separated list of tables to load")
def load_data(layers):
    """Load FWA_BC.gdb to PostgreSQL
    """
    fwa = fwakit.FWA()
    # create required extenstions/functions/schema if they don't exist
    fwa.db.execute("CREATE EXTENSION IF NOT EXISTS POSTGIS")
    fwa.db.execute("CREATE EXTENSION IF NOT EXISTS LTREE")
    fwa.db.execute("CREATE SCHEMA IF NOT EXISTS {s}".format(s=fwa.schema))
    fwa.db.execute(fwa.queries["functions"])
    # load source data
    click.echo('Loading FWA source data to PostgreSQL database')
    if not layers:
        in_layers = fwa.config["layers"]
    else:
        layers = layers.split(",")
        in_layers = [l for l in fwa.config["layers"] if l["table"] in layers]
    for layer in in_layers:
        click.echo('Loading '+layer['alias']+' to '+layer['table'])
        # load layer
        util.gdb2pg(fwa.config["source_file"],
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
        click.echo("Adding indexes to %s" % layer["table"])
        # create indexes on columns noted in parameters
        for column in layer["fields"]:
            tablename = layer['table']
            fwa.db[table].create_index([column], tablename+"_"+column+"_idx")


cli.add_command(createdb)
cli.add_command(get_data)
cli.add_command(load_data)

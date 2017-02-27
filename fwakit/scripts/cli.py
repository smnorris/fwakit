# Command line interface for loading freshwater atlas data
from __future__ import absolute_import
import urlparse
import tempfile
import urllib2
import zipfile
import os

import click

import pgdb
import fwakit


def make_sure_path_exists(path):
    """
    Make directories in path if they do not exist.
    Modified from http://stackoverflow.com/a/5032238/1377021
    """
    try:
        os.makedirs(path)
        return path
    except:
        pass


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
    db.execute("CREATE DATABASE {db}".format(db=dbname))


@click.command()
def download():
    """Download FWA gdb archives from GeoBC ftp
    """
    fwa = fwakit.FWA()

    # download files from ftp
    for source_file in fwa.config['sources']:
        click.echo('Downloading '+source_file)
        fp = tempfile.NamedTemporaryFile('wb',
                                         dir=tempfile.gettempdir(),
                                         suffix=".zip",
                                         delete=False)
        download = urllib2.urlopen(source_file)
        file_size_dl = 0
        block_sz = 8192
        while True:
            buffer = download.read(block_sz)
            if not buffer:
                break
            file_size_dl += len(buffer)
            fp.write(buffer)
        fp.close()

        # unzip the gdb, delete zip archive
        click.echo('Extracting '+source_file+' to '+fwa.config['dl_path '])
        unzip_dir = make_sure_path_exists(fwa.config["dl_path"])
        zipped_file = zipfile.ZipFile(fp.name, 'r')
        zipped_file.extractall(unzip_dir)
        zipped_file.close()

        # delete the temporary zipfile
        os.unlink(fp.name)


@click.command()
@click.option('--layers', '-l', help="Comma separated list of tables to load")
@click.option('--skiplayers', '-sl', help="Comma separated list of tables to skip")
def load(layers, skiplayers):
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
    if skiplayers:
        skiplayers = skiplayers.split(",")
        skip_layers = [l for l in fwa.config["layers"] if l["table"] in skiplayers]
        in_layers = list(set(in_layers).difference(set(skip_layers)))
    for layer in in_layers:
        click.echo('Loading '+layer['alias']+' to '+layer['table'])
        # load layer
        fwa.db.ogr2pg(os.path.join(fwa.config["dl_path"], layer["source"]),
                      in_layer=layer['table'].upper(),
                      out_layer=layer['table'],
                      schema=fwa.schema)


@click.command()
@click.option('--layers', '-l', help="Comma separated list of tables to load")
@click.option('--skiplayers', '-sl', help="Comma separated list of tables to skip")
def index(layers, skiplayers):
    """Clean and index FWA_BC.gdb in PostgreSQL
    """
    fwa = fwakit.FWA()
    click.echo('Modifying and indexing FWA tables in PostgreSQL database')
    if not layers:
        in_layers = fwa.config["layers"]
    else:
        layers = layers.split(",")
        in_layers = [l for l in fwa.config["layers"] if l["table"] in layers]
    if skiplayers:
        skiplayers = skiplayers.split(",")
        skip_layers = [l['alias'] for l in fwa.config["layers"] if l["table"] in skiplayers]
        in_layers = [l for lin in in_layers if l['alias'] not in skip_layers]
    for layer in in_layers:
        click.echo('Cleaning '+layer['table'])
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
        # re-create geometry index
        if 'geom' in fwa.db[table].columns:
            fwa.db[table].create_index_geom()
    # create a simplified 20k-50k lookup table
    fwa.create_lut_50k_20k_wsc()


cli.add_command(createdb)
cli.add_command(download)
cli.add_command(load)
cli.add_command(index)

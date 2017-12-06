# Command line interface for loading freshwater atlas data
from __future__ import absolute_import
try:
    from urllib.parse import urljoin
except ImportError:
     from urlparse import urljoin

import os

import click
import fiona

import fwakit
from fwakit import util


def parse_layers(fwa, layers, skiplayers):
    if not layers:
        in_layers = fwa.tables
    else:
        layers = layers.split(",")
        in_layers = [l for l in fwa.tables if l in layers]
    if skiplayers:
        skiplayers = skiplayers.split(",")
        skip_layers = [l for l in fwa.tables if l in skiplayers]
        in_layers = list(set(in_layers).difference(set(skip_layers)))
    return in_layers


@click.group()
def cli():
    pass


@click.command()
@click.option('--files', '-f', help='List of files to download')
@click.option('--source_url', '-u', help='URL to download from')
@click.option('--dl_path', '-p', help='Local path to download files to')
def download(files, source_url, dl_path):
    """Download FWA gdb archives from GeoBC ftp
    """
    fwa = fwakit.FWA()
    # download files from ftp
    if not source_url:
        source_url = fwa.config['source_url']
    if not dl_path:
        dl_path = fwa.config['dl_path']
    # check the url string, it must have a trailing /
    if source_url[-1] != '/':
        source_url = source_url+"/"
    if files:
        files = files.split(",")
    else:
        files = fwa.config['source_files']
    for source_file in files:
        url = urljoin(source_url, source_file)
        click.echo('Downloading '+source_file)
        util.download_and_unzip(url, dl_path)


@click.command()
@click.option('--layers', '-l', help='Comma separated list of tables to load')
@click.option('--skiplayers', '-sl',
              help='Comma separated list of tables to skip')
@click.option('--dl_path', '-p', help='Local path to download files to')
def load(layers, skiplayers, dl_path, default=fwakit.config['dl_path']):
    """Load FWA data to PostgreSQL
    """
    fwa = fwakit.FWA()
    # parse the input layers
    in_layers = parse_layers(fwa, layers, skiplayers)
    # create required extenstions/functions/schema if they don't exist
    fwa.db.execute('CREATE EXTENSION IF NOT EXISTS POSTGIS')
    fwa.db.execute('CREATE EXTENSION IF NOT EXISTS LTREE')
    fwa.db.execute('CREATE SCHEMA IF NOT EXISTS {s}'.format(s=fwa.schema))
    fwa.db.execute(fwa.queries['functions'])

    click.echo('Loading FWA source data to PostgreSQL database')
    # iterate through all data specified in config, loading only tables specified
    for source_file in fwa.config['source_files']:
        source_gdb = os.path.join(fwa.config['dl_path'],
                                  os.path.splitext(source_file)[0])
        for table in fwa.config['source_files'][source_file]:
            if table in in_layers:
                if 'grouped' in fwa.config['source_files'][source_file][table].keys():
                    click.echo('Loading %s by watershed group' % table)
                    groups = [g for g in fiona.listlayers(source_gdb) if g[0] != '_']
                    fwa.db[fwa.schema+"."+table].drop()
                    for i, group in enumerate(sorted(groups)):
                        click.echo(group)
                        fwa.db.ogr2pg(source_gdb,
                                      in_layer=group,
                                      out_layer=group.lower(),
                                      schema=fwa.schema,
                                      dim=3)
                        # combine the groups into a single table
                        if i == 0:
                            sql = '''CREATE TABLE {schema}.{table} AS
                                     SELECT * FROM {schema}.{g} LIMIT 0
                                  '''.format(schema=fwa.schema, table=table,
                                             g=group.lower())
                            fwa.db.execute(sql)
                        sql = '''INSERT INTO {schema}.{table}
                                 SELECT * FROM {schema}.{g}
                              '''.format(schema=fwa.schema,
                                         table=table,
                                         g=group.lower())
                        fwa.db.execute(sql)
                        # drop the source group table
                        fwa.db[fwa.schema+"."+group.lower()].drop()
                else:
                    click.echo('Loading ' + table)
                    fwa.db.ogr2pg(source_gdb,
                                  in_layer=table.upper(),
                                  out_layer=table,
                                  schema=fwa.schema,
                                  dim=3)


@click.command()
@click.option('--layers', '-l', help='Comma separated list of tables to index')
@click.option('--skiplayers', '-sl', help='Comma separated list of tables to skip')
def index(layers, skiplayers):
    """Clean and index FWA data
    """
    fwa = fwakit.FWA()
    in_layers = parse_layers(fwa, layers, skiplayers)

    for layer in in_layers:
        click.echo('Cleaning ' + fwa.tables[layer])
        # drop ogr and esri columns
        table = fwa.tables[layer]
        for column in ['ogc_fid', 'geometry_area', 'geometry_length']:
            if column in fwa.db[table].columns:
                fwa.db[table].drop_column(column)
        # ensure _id primary/foreign keys are int - ogr maps them to floats
        # integer should be fine for all but linear_feature_id
        for column in fwa.db[table].columns:
            if column[-3:] == '_id':
                if column == 'linear_feature_id':
                    column_type = 'bigint'
                else:
                    column_type = 'integer'
                sql = '''ALTER TABLE {t} ALTER COLUMN {col} TYPE {type}
                      '''.format(t=table, col=column, type=column_type)
                fwa.db.execute(sql)
        # make sure there are no '<Null>' strings in codes
        for column in ['fwa_watershed_code', 'local_watershed_code']:
            if column in fwa.db[table].columns:
                sql = """UPDATE {t} SET {c} = NULL WHERE {c} = '<Null>'
                      """.format(t=table, c=column)
                fwa.db.execute(sql)
        # add ltree columns to tables with watershed codes
        if 'fwa_watershed_code' in fwa.db[table].columns:
            click.echo('Adding ltree types and indexes')
            fwa.add_ltree(table)
        # add primary key constraint
        fwa.db[table].add_primary_key(fwa.config[layer]['id'])
        click.echo('Adding indexes to %s' % table)
        # create indexes on columns noted in parameters
        for column in fwa.config[layer]['index_fields']:
            tablename = table.split('.')[1]
            fwa.db[table].create_index([column], tablename+'_'+column+'_idx')
        # re-create geometry index
        if 'geom' in fwa.db[table].columns:
            fwa.db[table].create_index_geom()
    # create a simplified 20k-50k lookup table
    if 'fwa_streams_20k_50k_wsc' in fwa.db.tables_in_schema(fwa.schema):
        fwa.create_lut_50k_20k_wsc()


cli.add_command(download)
cli.add_command(load)
cli.add_command(index)

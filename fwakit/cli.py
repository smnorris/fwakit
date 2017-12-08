# Command line interface for loading freshwater atlas data
from __future__ import absolute_import
try:
    from urllib.parse import urljoin
except ImportError:
     from urlparse import urljoin

import os

import click
import fiona

import fwakit as fwa


def parse_layers(layers, skiplayers):
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


def validate_format(ctx, param, value):
    if value in ['GPKG', 'FileGDB']:
        return value
    else:
        raise click.BadParameter("Format '{}' is not supported".format(value))


@click.group()
def cli():
    pass


@cli.command()
@click.option('--files', '-f', help='List of files to download')
@click.option('--source_url', '-u', help='URL to download from',
              default=fwa.config['source_url'])
@click.option('--dl_path', '-p', help='Local path to download files to',
              default=fwa.config['dl_path'])
def download(files, source_url, dl_path):
    """Download FWA gdb archives from GeoBC ftp
    """
    # download files from internet
    # check the url string, it must have a trailing /
    if source_url[-1] != '/':
        source_url = source_url+"/"
    if files:
        files = files.split(",")
    else:
        files = fwa.config['data_def']
    for source_file in files:
        url = urljoin(source_url, source_file)
        click.echo('Downloading '+source_file)
        fwa.util.download_and_unzip(url, dl_path)


@cli.command()
@click.option('--layers', '-l', help='Comma separated list of tables to load')
@click.option('--skiplayers', '-sl',
              help='Comma separated list of tables to skip')
@click.option('--dl_path', '-p', help='Local path to download files to')
def load(layers, skiplayers, dl_path, default=fwa.config['dl_path']):
    """Load FWA data to PostgreSQL
    """
    db = fwa.util.connect()
    # parse the input layers
    in_layers = parse_layers(layers, skiplayers)
    # create required extenstions/functions/schema if they don't exist
    db.execute('CREATE EXTENSION IF NOT EXISTS POSTGIS')
    db.execute('CREATE EXTENSION IF NOT EXISTS LTREE')
    db.execute('CREATE SCHEMA IF NOT EXISTS whse_basemapping')
    db.execute(fwa.queries['functions'])

    click.echo('Loading FWA source data to PostgreSQL database')
    # iterate through all data specified in config, loading only tables specified
    for source_file in fwa.config['data_def']:
        source_gdb = os.path.join(fwa.config['dl_path'],
                                  os.path.splitext(source_file)[0])
        for table in fwa.config['source_files'][source_file]:
            if table in in_layers:
                if 'grouped' in fwa.config['source_files'][source_file][table].keys():
                    click.echo('Loading %s by watershed group' % table)
                    groups = [g for g in fiona.listlayers(source_gdb) if g[0] != '_']
                    db['whse_basemapping.'+table].drop()
                    for i, group in enumerate(sorted(groups)):
                        click.echo(group)
                        db.ogr2pg(source_gdb,
                                  in_layer=group,
                                  out_layer=group.lower(),
                                  schema='whse_basemapping',
                                  dim=3)
                        # combine the groups into a single table
                        if i == 0:
                            sql = '''CREATE TABLE whse_basemapping.{table} AS
                                     SELECT * FROM whse_basemapping.{g} LIMIT 0
                                  '''.format(table=table, g=group.lower())
                            db.execute(sql)
                        sql = '''INSERT INTO whse_basemapping.{table}
                                 SELECT * FROM whse_basemapping.{g}
                              '''.format(table=table, g=group.lower())
                        db.execute(sql)
                        # drop the source group table
                        db['whse_basemapping.'+group.lower()].drop()
                else:
                    click.echo('Loading ' + table)
                    db.ogr2pg(source_gdb,
                              in_layer=table.upper(),
                              out_layer=table,
                              schema='whse_basemapping',
                              dim=3)

    # Clean and index the data
    for layer in in_layers:
        click.echo('Cleaning ' + fwa.tables[layer])
        # drop ogr and esri columns
        table = fwa.tables[layer]
        for column in ['ogc_fid', 'geometry_area', 'geometry_length']:
            if column in db[table].columns:
                db[table].drop_column(column)
        # ensure _id primary/foreign keys are int - ogr maps them to floats
        # integer should be fine for all but linear_feature_id
        for column in db[table].columns:
            if column[-3:] == '_id':
                if column == 'linear_feature_id':
                    column_type = 'bigint'
                else:
                    column_type = 'integer'
                sql = '''ALTER TABLE {t} ALTER COLUMN {col} TYPE {type}
                      '''.format(t=table, col=column, type=column_type)
                db.execute(sql)
        # make sure there are no '<Null>' strings in codes
        for column in ['fwa_watershed_code', 'local_watershed_code']:
            if column in db[table].columns:
                sql = """UPDATE {t} SET {c} = NULL WHERE {c} = '<Null>'
                      """.format(t=table, c=column)
                db.execute(sql)
        # add ltree columns to tables with watershed codes
        if 'fwa_watershed_code' in db[table].columns:
            click.echo('Adding ltree types and indexes')
            fwa.add_ltree(table)
        # add primary key constraint
        db[table].add_primary_key(fwa.config[layer]['id'])
        click.echo('Adding indexes to %s' % table)
        # create indexes on columns noted in parameters
        for column in fwa.config[layer]['index_fields']:
            tablename = table.split('.')[1]
            db[table].create_index([column], tablename+'_'+column+'_idx')
        # re-create geometry index
        if 'geom' in db[table].columns:
            db[table].create_index_geom()
    # create lookups
    # - simplified 20k-50k lookup
    # - lut of streams with codes that cannot be used for up-downstream queries
    db.execute(fwa.queries['create_lut_50k_20k_wsc'])
    db.execute(fwa.queries['create_invalid_codes'])


@cli.command()
@click.option('--out_path', '-o', type=click.Path(exists=True), help='Path to dump .gdb files')
@click.option('--tables', '-t', help='Comma separated list of tables to dump',
              default='fwa_stream_networks_sp,fwa_watershed_groups_poly,fwa_lakes_poly')
@click.option('--wsg', '-g', default='VICT', help='Watershed group code to dump')
@click.option('--out_format', '-of', default='GPKG', callback=validate_format,
              help='Output (ogr) format. Default GPKG (Geopackage)')
def dump(out_path, tables, wsg, out_format):
    """Dump sample data to file
    """
    db = fwa.util.connect()
    dump_tables = tables
    for source_file in fwa.config['data_def']:
        for table in fwa.config['data_def'][source_file]:
            if table in dump_tables:
                # out file name is taken directly from config
                out_file = os.path.splitext(source_file)[0]
                # prepend out path to file name
                if out_path:
                    out_file = os.path.join(out_path, out_file)
                # modify the file extension if writing to gpkg
                if out_format == 'GPKG':
                    out_file = out_file.replace('.gdb', '.gpkg')
                # get geometry type if dumping to gdb
                if out_format == 'FileGDB':
                    sql = """SELECT geometrytype(geom)
                             FROM {t} LIMIT 1""".format(t=fwa.tables[table])
                    geom_type = db.query(sql).fetchone()[0]
                else:
                    geom_type = None
                columns = db[fwa.tables[table]].columns
                # don't try and dump ltree types
                columns = [c for c in columns if 'ltree' not in c]
                sql = """SELECT {c} FROM {t}
                         WHERE watershed_group_code = '{g}'
                      """.format(c=', '.join(columns),
                                 t=fwa.tables[table],
                                 g=wsg)
                if 'grouped' not in fwa.config['data_def'][source_file][table].keys():
                    outlayer = table
                else:
                    outlayer = wsg
                db.pg2ogr(sql, out_format, out_file, outlayer=outlayer,
                          geom_type=geom_type)


if __name__ == '__main__':
    cli()

# Command line interface for loading freshwater atlas data
from __future__ import absolute_import
try:
    from urllib.parse import urljoin
except ImportError:
     from urlparse import urljoin

import os

import click

import pgdata
import fwakit as fwa
from . import settings


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
@click.option('--db_url', '-db', help='Database to load files to',
              envvar='FWA_DB')
def create_db(db_url):
    """Create a fresh database, install extensions and create schema
    """
    pgdata.create_db(db_url)
    db = pgdata.connect(db_url)

    # create required extenstions/functions/schema if they don't exist
    db.execute('CREATE EXTENSION IF NOT EXISTS postgis')
    db.execute('CREATE EXTENSION IF NOT EXISTS lostgis')
    db.execute('CREATE EXTENSION IF NOT EXISTS ltree')
    db.execute('CREATE SCHEMA IF NOT EXISTS whse_basemapping')


@cli.command()
@click.option('--files', '-f', help='List of files to download')
@click.option('--source_url', '-u', help='URL to download from',
              default=settings.source_url)
@click.option('--dl_path', '-p', help='Local path to download files to',
              default=settings.dl_path)
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
        files = settings.sources_dict
    for source_file in files:
        url = urljoin(source_url, source_file)
        click.echo('Downloading '+source_file)
        fwa.util.download_and_unzip(url, dl_path)


@cli.command()
@click.option('--layers', '-l', help='Comma separated list of tables to load')
@click.option('--skiplayers', '-sl',
              help='Comma separated list of tables to skip')
@click.option('--dl_path', '-p', help='Local path to download files to',
              default=settings.dl_path)
@click.option('--db_url', '-db', help='Database to load files to',
              envvar='FWA_DB')
@click.option('--wsg', '-g', help='List of group codes to load')
def load(layers, skiplayers, dl_path, db_url, wsg):
    """Load FWA data to PostgreSQL
    """
    # make sure the db, extensions, schemas exist
    create_db(db_url)

    db = fwa.util.connect(db_url)
    # parse the input layers
    in_layers = parse_layers(layers, skiplayers)

    # create wsc parsing functions
    db.execute(fwa.queries['fwa_wscode2ltree'])
    db.execute(fwa.queries['fwa_trimwsc'])

    click.echo('Loading FWA source data to PostgreSQL database')
    # iterate through all data specified in config, loading only tables specified
    for source_file in settings.sources_dict:
        source_gdb = os.path.join(dl_path,
                                  os.path.splitext(source_file)[0])
        # load data that is not split up by watershed group
        for table in settings.sources_dict[source_file]:
            if table in in_layers:
                if 'grouped' not in settings.sources_dict[source_file][table].keys():
                    click.echo('Loading ' + table)
                    if not wsg:
                        db.ogr2pg(source_gdb,
                                  in_layer=table.upper(),
                                  out_layer=table,
                                  schema='whse_basemapping',
                                  dim=3)
                    else:
                        db.ogr2pg(source_gdb,
                                  in_layer=table.upper(),
                                  out_layer=table,
                                  schema='whse_basemapping',
                                  sql='watershed_group_code IN ({g})'.format(g=wsg),
                                  dim=3)
        # load data that *is* split up by watershed group
        for table in settings.sources_dict[source_file]:
            if table in in_layers:
                if 'grouped' in settings.sources_dict[source_file][table].keys():
                    click.echo('Loading %s by watershed group' % table)
                    db['whse_basemapping.'+table].drop()
                    if wsg:
                        groups = wsg.split(',')
                    else:
                        groups = fwa.list_groups(db=db)
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

    # Clean and index the data
    for source_file in settings.sources_dict:
        for layer in settings.sources_dict[source_file]:
            if layer in in_layers:
                click.echo(fwa.tables[layer]+': cleaning')
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
                    click.echo(fwa.tables[layer]+': adding ltree types')
                    fwa.add_ltree(table, db=db)
                # add primary key constraint
                db[table].add_primary_key(settings.sources_dict[source_file][layer]['id'])
                click.echo(fwa.tables[layer]+': indexing')
                # create indexes on columns noted in parameters
                for column in settings.sources_dict[source_file][layer]['index_fields']:
                    tablename = table.split('.')[1]
                    db[table].create_index([column], tablename+'_'+column+'_idx')
                # re-create geometry index
                if 'geom' in db[table].columns:
                    db[table].create_index_geom()

    # create additional functions, convenience tables, lookups
    # (run queries with 'create_' prefix if required sources are present)

    # create upstream/downstream functions and invalid code lookup
    if 'whse_basemapping.fwa_stream_networks_sp' in db.tables:
        for func in fwa.queries:
            if func[:4] == 'fwa_':
                click.echo(func)
                db.execute(fwa.queries[func])
        db.execute(fwa.queries['create_invalid_codes'])

    # create named streams table
    if ('whse_basemapping.fwa_stream_networks_sp' in db.tables and
            'whse_basemapping.fwa_lakes_poly' in db.tables and
            'whse_basemapping.fwa_manmade_waterbodies_poly' in db.tables):
        db.execute(fwa.queries['named_streams'])

    # subdivide watershed group polys
    if 'whse_basemapping.fwa_watershed_groups_poly' in db.tables:
        db.execute(fwa.queries['create_fwa_watershed_groups_subdivided'])

    # simplify the 20k-50k stream lookup
    if 'whse_basemapping.fwa_streams_20k_50k' in db.tables:
        db.execute(fwa.queries['create_lut_50k_20k_wsc'])


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
    for source_file in settings.sources_dict:
        for table in settings.sources_dict[source_file]:
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
                if 'grouped' not in settings.sources_dict[source_file][table].keys():
                    outlayer = table
                else:
                    outlayer = wsg
                db.pg2ogr(sql, out_format, out_file, outlayer=outlayer,
                          geom_type=geom_type)


if __name__ == '__main__':
    cli()

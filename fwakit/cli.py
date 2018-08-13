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
    """Create a fresh database/schema
    """
    pgdata.create_db(db_url)
    db = pgdata.connect(db_url)
    db.execute('CREATE EXTENSION IF NOT EXISTS postgis')
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
        files = settings.source_files
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
    db = fwa.util.connect(db_url)
    # parse the input layers
    in_layers = parse_layers(layers, skiplayers)

    # create wsc parsing functions
    db.execute(fwa.queries['fwa_trimwsc'])
    db.execute(fwa.queries['fwa_wsc2ltree'])

    click.echo('Loading FWA source data to PostgreSQL database')

    # load watershed groups first, it is required for processing subsequent data
    if 'whse_basemapping.fwa_watershed_groups_poly' not in db.tables:
        layer = [t for t in settings.source_tables if t['table'] == 'fwa_watershed_groups_poly'][0]
        click.echo('Loading '+layer['table'])
        gdb = os.path.join(dl_path, os.path.splitext(layer['source_file'])[0])
        if not os.path.exists(gdb):
            raise IOError(gdb+' does not exist, download it first')
        db.ogr2pg(gdb,
                  in_layer=layer['table'].upper(),
                  out_layer=layer['table'],
                  schema='whse_basemapping',
                  dim=2)

    # iterate through the rest of the data specified in config (and command option)
    for layer in settings.source_tables:
        table = fwa.tables[layer['table']]
        if layer['table'] in in_layers:
            gdb = os.path.join(dl_path, os.path.splitext(layer['source_file'])[0])
            if os.path.exists(gdb):
                # load data that is not split up by watershed group
                if not layer['grouped']:
                    click.echo('Loading %s' % layer['table'])
                    # don't overwrite these
                    if table not in db.tables:
                        # only load the group of interest if specified
                        if not wsg:
                            db.ogr2pg(gdb,
                                      in_layer=layer['table'].upper(),
                                      out_layer=layer['table'],
                                      schema='whse_basemapping',
                                      dim=3)
                        else:
                            db.ogr2pg(gdb,
                                      in_layer=layer['table'].upper(),
                                      out_layer=layer['table'],
                                      schema='whse_basemapping',
                                      sql='watershed_group_code IN ({g})'.format(g=wsg),
                                      dim=3)
                # load data that *is* split up by watershed group
                else:
                    click.echo('Loading %s by watershed group' % layer['table'])
                    # overwrite if the table exists
                    db[table].drop()
                    if wsg:
                        groups = wsg.split(',')
                    else:
                        groups = fwa.list_groups(db=db)
                    for i, group in enumerate(sorted(groups)):
                        click.echo(group)
                        db.ogr2pg(gdb,
                                  in_layer=group,
                                  out_layer=layer['table']+'_'+group.lower(),
                                  schema='whse_basemapping',
                                  dim=3)
                        # combine the groups into a single table
                        if i == 0:
                            sql = '''CREATE TABLE whse_basemapping.{table} 
                                     (LIKE whse_basemapping.{g})
                                  '''.format(table=layer['table'],
                                             g=layer['table']+'_'+group.lower())
                            db.execute(sql)
                        sql = '''INSERT INTO whse_basemapping.{table}
                                 SELECT * FROM whse_basemapping.{g}
                              '''.format(table=layer['table'],
                                         g=layer['table']+'_'+group.lower())
                        db.execute(sql)
                        # drop the source group table
                        db['whse_basemapping.'+layer['table']+'_'+group.lower()].drop()
            else:
                click.echo("""{l}: source file {f} does not exist, skipping"""
                           .format(l=layer['table'],
                                   f=layer['source_file']))


@cli.command()
@click.option('--layers', '-l', help='Comma separated list of tables to clean')
@click.option('--skiplayers', '-sl',
              help='Comma separated list of tables to skip')
@click.option('--db_url', '-db', help='Database to load files to',
              envvar='FWA_DB')
def clean(layers, skiplayers, db_url):
    """Clean and index the data after load
    """
    db = fwa.util.connect(db_url)
    # parse the input layers
    in_layers = parse_layers(layers, skiplayers)
    for layer in settings.source_tables:
        table = fwa.tables[layer['table']]
        if layer['table'] in in_layers and table in db.tables:
            click.echo(layer['table']+': cleaning')
            # drop ogr and esri columns
            for column in settings.drop_columns:
                if column in db[table].columns:
                    db[table].drop_column(column)
            # ensure _id keys are int - ogr maps them to double
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
                click.echo(layer['table']+': adding ltree types')
                fwa.add_ltree(table, db=db)
            # add primary key constraint
            db[table].add_primary_key(layer['id'])
            click.echo(layer['table']+': indexing')
            # create indexes on columns noted in parameters
            for column in layer['index_fields']:
                db[table].create_index([column])
            # create geometry index for tables loaded by group
            if layer['grouped']:
                db[table].create_index_geom()
            # index watershed codes
            for col in ['fwa_watershed_code', 'local_watershed_code']:
                if col in db[table].columns:
                    sql = """CREATE INDEX IF NOT EXISTS ix_{n}_{c}_tpo ON {t} ({c} text_pattern_ops)
                              """.format(n=layer['table'], t=table, c=col)
                    db.execute(sql)

    # create additional functions, convenience tables, lookups
    # (run queries with 'create_' prefix if required sources are present)
    # create general upstream / downstream functions based on watershed codes
    db.execute(fwa.queries['fwa_upstreamwsc'])
    #db.execute(fwa.queries['fwa_downstream'])
    # for streams, create length upstream/downstream functions and invalid code lookup
    if 'whse_basemapping.fwa_stream_networks_sp' in db.tables:
        db.execute(fwa.queries['create_invalid_codes'])
        for f in ['fwa_lengthdownstream',
                  'fwa_lengthupstream',
                  'fwa_lengthinstream']:
            db.execute(fwa.queries[f])

    # create named streams table
    if ('whse_basemapping.fwa_stream_networks_sp' in db.tables and
            'whse_basemapping.fwa_lakes_poly' in db.tables and
            'whse_basemapping.fwa_manmade_waterbodies_poly' in db.tables):
        db.execute(fwa.queries['create_fwa_named_streams'])

    # subdivide watershed group polys
    if 'whse_basemapping.fwa_watershed_groups_poly' in db.tables:
        db.execute(fwa.queries['create_fwa_watershed_groups_subdivided'])

    # simplify the 20k-50k stream lookup
    if 'whse_basemapping.fwa_streams_20k_50k' in db.tables:
        db.execute(fwa.queries['create_lut_50k_20k_wsc'])

    # create a simple waterbody lookup table
    if ('whse_basemapping.fwa_wetlands_poly' in db.tables and
            'whse_basemapping.fwa_lakes_poly' in db.tables and
            'whse_basemapping.fwa_manmade_waterbodies_poly' in db.tables and
            'whse_basemapping.fwa_rivers_poly' in db.tables):
        db.execute(fwa.queries['create_fwa_waterbodies'])

    # create text_pattern_pos indexes on watershed codes
    # (these aren't included in sources.json indexes as index type is required)



@cli.command()
@click.option('--out_path', '-o', type=click.Path(exists=True),
              help='Path to dump .gdb files')
@click.option('--layers', '-l',
              help='Comma separated list of layers/tables to dump',
              default=','.join(['fwa_watershed_groups_poly',
                                'fwa_lakes_poly',
                                'fwa_stream_networks_sp',
                                'fwa_linear_boundaries_sp',
                                'fwa_watersheds_poly_sp',
                                'fwa_rivers_poly',
                                'fwa_manmade_waterbodies_poly']))
@click.option('--skiplayers', '-sl',
              help='Comma separated list of tables to skip')
@click.option('--wsg', '-g', default='VICT',
              help='Watershed group code to dump')
@click.option('--out_format', '-of', default='GPKG', callback=validate_format,
              help='Output (ogr) format. Default GPKG (Geopackage)')
def dump(out_path, layers, skiplayers, wsg, out_format):
    """Dump sample data to file
    """
    db = fwa.util.connect()
    # parse the input layers
    dump_tables = parse_layers(layers, skiplayers)
    for layer in settings.source_tables:
        if layer['table'] in dump_tables:
            # out file name is taken directly from config
            out_file = os.path.splitext(layer['source_file'])[0]
            # prepend out path to file name
            if out_path:
                out_file = os.path.join(out_path, out_file)
            # modify the file extension if writing to gpkg
            if out_format == 'GPKG':
                out_file = out_file.replace('.gdb', '.gpkg')
            # get geometry type if dumping to gdb
            if out_format == 'FileGDB':
                sql = """SELECT geometrytype(geom)
                         FROM {t} LIMIT 1""".format(t=fwa.tables[layer['table']])
                geom_type = db.query(sql).fetchone()[0]
            else:
                geom_type = None
            columns = db[fwa.tables[layer['table']]].columns
            # don't try and dump ltree types
            columns = [c for c in columns if 'ltree' not in c]
            sql = """SELECT {c} FROM {t}
                     WHERE watershed_group_code = '{g}'
                  """.format(c=', '.join(columns),
                             t=fwa.tables[layer['table']],
                             g=wsg)
            if not layer['grouped']:
                outlayer = layer['table']
            else:
                outlayer = wsg
            db.pg2ogr(sql, out_format, out_file, outlayer=outlayer,
                      geom_type=geom_type)


if __name__ == '__main__':
    cli()

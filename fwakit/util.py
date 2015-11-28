import subprocess
import urlparse
import os

import fiona


def gdb2pg(gdb, layer, dburl, schema='temp'):
    """Use ogr2ogr to load source data to postgres"""
    # some tables don't have have geometry but ogr2ogr is still useful
    with fiona.drivers():
        with fiona.open(gdb, layer=layer) as src:
            if src.schema["geometry"] != 'None':
                geom = "-lco GEOMETRY_NAME=geom"
            else:
                geom = ""
    # break down the db url
    u = urlparse.urlparse(dburl)
    command = """ogr2ogr \
                   -progress \
                   --config PG_USE_COPY YES \
                   -t_srs EPSG:3005 \
                   -f PostgreSQL \
                   PG:'host={host} user={user} dbname={db} password={pwd}' \
                   -lco OVERWRITE=YES \
                   -lco SCHEMA={schema} \
                   {geom} \
                   -nln {lowerlayer} \
                   {gdb} {layer}
              """.format(host=u.hostname,
                         user=u.username,
                         db=u.path[1:],
                         pwd=u.password,
                         schema=schema,
                         geom=geom,
                         lowerlayer=layer.lower(),
                         gdb=gdb,
                         layer=layer)
    subprocess.call(command, shell=True)


def csv2pg(infile, dburl, schema='temp', table=None):
    """Use csvkit to quickly load a data file to to postgres"""
    if not table:
        table = os.path.splitext(os.path.split(infile)[1])[0]
    command = """csvsql --db {dburl} \
                        --table {table} \
                        --insert {infile} \
                        --db-schema {schema} \
                        -y 100000
              """.format(dburl=dburl,
                         table=table,
                         infile=infile,
                         schema=schema)
    subprocess.call(command, shell=True)


def dump(dburl, sql, driver, outfile):
    """Dump a postgis query to file (shapefile or geojson)
       If dumping to geojson, use EPSG:4326
    """
    u = urlparse.urlparse(dburl)
    command = """ogr2ogr \
                    -progress \
                    -f "{driver}" \
                    {outfile} \
                    PG:'host={host} user={user} dbname={db} password={pwd}' \
                    -sql "{sql}"
              """.format(driver=driver,
                         host=u.hostname,
                         user=u.username,
                         db=u.path[1:],
                         pwd=u.password,
                         outfile=outfile,
                         sql=sql)
    # translate GeoJSON to EPSG:4326
    if driver == 'GeoJSON':
        command = command.replace("""-f "GeoJSON" """,
                                  """-f "GeoJSON" -t_srs EPSG:4326""")
    subprocess.call(command, shell=True)

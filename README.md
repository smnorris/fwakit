# fwakit

Python / PostgreSQL tools for working with the [BC Freshwater Atlas](http://geobc.gov.bc.ca/base-mapping/atlas/fwa/)

## Requirements
- PostgreSQL
- PostGIS
- GDAL
- pgdb

## Installation
`pip install fwakit`

## Setup
Use the CLI to set up an FWA database:  

- edit `config.yml` as necessary
- create `db_url` database specified in `config.yml` if it does not exist
- download data, load to postgres and create indexes  

```
$ fwakit --help
Usage: fwakit [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  download  Download FWA gdb archives from GeoBC ftp
  index     Clean and index FWA data
  load      Load FWA data to PostgreSQL
$ fwakit download
$ fwakit load
$ fwakit index
```

## Usage
```
# create fwa object
fwa = fwakit.FWA()

# do fwa stuff
groups = fwa.list_groups()
fwa.create_events_from_points('my_point_table', 'point_id', 'out_events', 50)
fwa.index_events_downstream('out_events', 'point_id')
```
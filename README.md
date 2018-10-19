# fwakit

Python/PostgreSQL tools for working with British Columbia's [Freshwater Atlas](https://www2.gov.bc.ca/gov/content/data/geographic-data-services/topographic-data/freshwater)

## Requirements
- Python 2.7+ (tested with 2.7.14, 3.6.4)
- PostgreSQL/PostGIS (tested with 10.2/2.4.2)
- GDAL (for loading data to PostgreSQL, tested with 2.2.3)

## Installation

`$ pip install fwakit`

## Configuration
Create an environment variable `FWA_DB` and set it to the SQLAlchemy db url for your database. For example:

MacOS/Linux etc:
`export FWA_DB=postgresql://postgres:postgres@localhost:5432/fwadb`

Windows:
`SET FWA_DB="postgresql://postgres:postgres@localhost:5432/fwadb"`

For more configuration, see `settings.py`.

## Setup

Get FWA data from GeoBC:

`$ fwakit download`

Note that the download may not work if you are behind a network proxy. If it fails, manually download and unzip the files of interest from [DataBC's ftp server](ftp://ftp.geobc.gov.bc.ca/sections/outgoing/bmgs/FWA_Public).

Create db, load FWA data, repair, index, and optimize:

```
$ fwakit create_db
$ fwakit load
$ fwakit clean
```

Note that because the tool downloads the entire set of FWA files to disk and then duplicates the data in postgres, ~20G or so of free disk space is required. If this is not available, run the load tool layer by layer or perhaps load the data directly from the ftp site using ogr2ogr and [GDAL VFS /vsicurl and /vsizip](http://www.gdal.org/gdal_virtual_file_systems.html)

## Usage

#### Use the Python module:

```
import fwakit as fwa

wsg = fwa.list_groups()

fwa.reference_points('point_table', 'point_id', 'event_table', 10)

```

#### Use installed `fwa` prefixed functions directly in postgresql:

```
fwakit_test=# SELECT fwa_upstreamlength(354136754, 1200) / 1000 as downstream_km, fwa_downstreamlength(354136754, 1200) / 1000 as upstream_km;
  downstream_km  |   upstream_km
-----------------+------------------
 5.1829073255008 | 9.48098793830257
(1 row)
```

#### Use `fwakit` command line interface for common tasks:

```
$ fwakit --help
Usage: fwakit [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  clean      Clean and index the data after load
  create_db  Create a fresh database/schema
  download   Download FWA gdb archives from GeoBC ftp
  dump       Dump sample data to file
  load       Load FWA data to PostgreSQL
```

#### Use data (created on load) for mapping and analysis, such as:

- `whse_basemapping.fwa_named_streams` - named streams, simplified and merged
- `whse_basemapping.fwa_watershed_groups_subdivided` - subdivided watershed groups, for much faster point in polygon queries


## Credits
- inspiration and code taken from [fiona](https://github.com/Toblerity/Fiona) and [osmnx](https://github.com/gboeing/osmnx)
- many thanks to GeoBC and the Ministry of Environment for building, maintaining, and publishing the [Freshwater Atlas](https://www2.gov.bc.ca/gov/content/data/geographic-data-services/topographic-data/freshwater)
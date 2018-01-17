# fwakit

Python/PostgreSQL tools for working with British Columbia's [Freshwater Atlas](https://www2.gov.bc.ca/gov/content/data/geographic-data-services/topographic-data/freshwater)

## requirements
- Python 2.7+ (tested with 2.7.14, 3.6.4)
- PostgreSQL/PostGIS (tested with 10.1/2.4.2)
- GDAL (for loading data to PostgreSQL, tested with 2.2.3)

## installation
`$ pip install fwakit`

## configuration
First, create a PostGIS enabled PostgreSQL database. For convenience, create an environment variable `FWA_DB` and set it to the SQLAlchemy db url for your database:

MacOS/Linux etc:
`export FWA_URL=postgresql://postgres:postgres@localhost:5432/fwadb`

Windows:
`SET FWA_URL="postgresql://postgres:postgres@localhost:5432/fwadb"`

For more configuration, see `settings.py`. 

## usage

Get FWA data from GeoBC:  
`$ fwakit download`

Note that this may not work if you are behind a network proxy. Download and unzip the files of interest manually from [ftp://ftp.geobc.gov.bc.ca/sections/outgoing/bmgs/FWA_Public](Data BC's ftp server).

Load all FWA data to postgres, repair, index, and optimize:  
`$ fwakit load`

Use fwakit in Python:
```
import fwakit as fwa
from fwakit import stream
from fwakit import lake

mystream = stream(blue_line_key=123456)
mylake = lake(waterbody_polygon_id=123456)
points = 'my_points_table'

fwa.dostuff(stream, points)
>>['stream','point','stuff1']
fwa.dostuff(lake, points)
>>['lake','point','stuff1','stuffa']
```
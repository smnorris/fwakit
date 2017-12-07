# fwakit

Python/PostgreSQL tools for working with British Columbia's [Freshwater Atlas](https://www2.gov.bc.ca/gov/content/data/geographic-data-services/topographic-data/freshwater)

## requirements
- Python 2.7+
- PostgreSQL/PostGIS

## installation
`$ pip install fwakit`

## configuration
Create a PostGIS enabled PostgreSQL database then edit `config.yml` to point to it via a SQLAlchemy db url:

`db_url: postgresql://postgres:postgres@localhost:5432/fwa_db_name`


## usage

Get FWA data from GeoBC:  
`$ fwakit download`

Load all FWA data to postgres, repair, index, optimize:  
`$ fwakit load`

Use fwakit in Python:
```
import fwakit
fwa = fwakit.FWA()
fwa.dostuff()
```
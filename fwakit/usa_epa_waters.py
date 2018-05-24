# adapted from
# https://github.com/donco/WatershedDelineation/blob/master/NavigationDelineationServices.py

import json

import requests

# See here for info for all WATERS web services
# https://www.epa.gov/waterdata/waters-web-services

# point service:
# https://www.epa.gov/waterdata/point-indexing-service
POINT_SERVICE_URL = "http://ofmpub.epa.gov/waters10/PointIndexing.Service?"

# delineation service:
# https://www.epa.gov/waterdata/navigation-delineation-service
WSD_DELINEATION_URL = "http://ofmpub.epa.gov/waters10/NavigationDelineation.Service?"


def index_point(x, y, tolerance):
    """
    Provided a location as lon, lat, find nearest NHD stream within tolerance
    Returns stream id, measure of location on stream, and distance from point to stream
    """
    parameters = {
        "pGeometry": "POINT(%s %s)" % (x, y),
        "pResolution": "2",
        "pPointIndexingMethod": "DISTANCE",
        "pPointIndexingMaxDist": str(tolerance),
        "pOutputPathFlag": "FALSE",
    }
    # make the resquest
    r = requests.get(
        POINT_SERVICE_URL,
        params=parameters)

    # parse the results
    comid = r.json()["output"]["ary_flowlines"][0]["comid"]
    measure = r.json()["output"]["ary_flowlines"][0]["fmeasure"]
    index_dist = r.json()["output"]["path_distance"]
    return(comid, measure, index_dist)


def delineate_watershed(feature_id, comid, measure, out_file):
    """
    Given a location as comid and measure, write boundary of watershed upstream of
    that location as geojson file
    """
    parameters = {
        "pNavigationType": "UT",
        "pStartComid": comid,
        "pStartMeasure": measure,
        "pMaxDistance": 560,
        "pFeatureType": "CATCHMENT",
        "pOutputFlag": "FEATURE",
        "pAggregationFlag": "TRUE",
        "optOutGeomFormat": "GEOJSON",
        "optOutPrettyPrint": 0
    }
    # make the resquest
    r = requests.get(
        WSD_DELINEATION_URL,
        params=parameters)

    if len(r.json()["output"]["shape"]["coordinates"]) == 1:
        geomtype = "Polygon"
    elif len(r.json()["output"]["shape"]["coordinates"]) > 1:
        geomtype = "MultiPolygon"

    x = {
        "type": "Feature",
        "properties": {
            "id": feature_id
        },
        "geometry": {
            "type": geomtype,
            "coordinates": r.json()["output"]["shape"]["coordinates"]
        }
    }
    with open(out_file, "w") as output:
        json.dump(x, output, indent=2)

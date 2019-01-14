import requests

BC_DEM_WCS_URL = "http://delivery.openmaps.gov.bc.ca/om/wcs"


def extract_dem(
    bounds,
    out_raster="dem.tif"
):
    """Get 25m DEM for area of interest from BC WCS, write to GeoTIFF
    """
    bbox = ",".join([str(b) for b in bounds])
    # build request
    payload = {
        "service": "WCS",
        "version": "1.0.0",
        "request": "GetCoverage",
        "coverage": "pub:bc_elevation_25m_bcalb",
        "Format": "GeoTIFF",
        "bbox": bbox,
        "CRS": "EPSG:3005",
        "resx": "25",
        "resy": "25",
    }
    # request data from WCS
    r = requests.get(BC_DEM_WCS_URL, params=payload)
    # save to tiff
    if r.status_code == 200:
        with open(out_raster, "wb") as file:
            file.write(r.content)
        return out_raster

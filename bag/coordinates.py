# Conversion of Rijksdriehoekscoordinaten (RD, EPSG:28992) to WGS84 latitude and longitude.
#
# The conversion uses the official transformation RDNAPTRANS2018 of NSGI, using the PROJ library (pyproj) and the
# correction grid in the 'proj' folder. This is accurate to a few centimeters.
#
# The result is in ETRS89, which is used as WGS84. The difference between ETRS89 and WGS84 is below one meter.
# See the notes in README.md.

import math
import os

try:
    import pyproj
except ImportError:
    pyproj = None

PROJ_FOLDER = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'proj')
GRID_FILE = os.path.join(PROJ_FOLDER, 'nl_nsgi_rdtrans2018.tif')

# Reference points to test the conversion: RD x, RD y, latitude, longitude. Calculated with RDNAPTRANS2018.
# The first point is the top of the Onze Lieve Vrouwetoren (Lange Jan) in Amersfoort.
# Without the grid file PROJ uses a less accurate transformation which is off by several decimeters.
REFERENCE_POINTS = [
    (155000, 463000, 52.155172887, 5.387203650),
    (121000, 487000, 52.369826305, 4.887970044),
    (180000, 311000, 50.788367103, 5.741705993),
]
MAX_ERROR_METERS = 0.03

_transformer = None


def _get_transformer():
    global _transformer
    if _transformer is None:
        # Make PROJ find the grid file. This is done once in every (multiprocessing) process.
        pyproj.datadir.append_data_dir(PROJ_FOLDER)
        _transformer = pyproj.Transformer.from_crs('EPSG:28992', 'EPSG:4258', always_xy=True)
    return _transformer


def rd_to_wgs84(x, y):
    """Convert RD coordinates to WGS84. Returns (latitude, longitude)."""
    lon, lat = _get_transformer().transform(x, y)
    return lat, lon


def distance_meters(lat1, lon1, lat2, lon2):
    """Approximate distance in meters between two nearby WGS84 points. Accurate enough for short distances."""
    return math.hypot((lat1 - lat2) * 111_200, (lon1 - lon2) * 111_200 * math.cos(math.radians(lat2)))


def check_requirements():
    """Check if the coordinate conversion works. Returns a list with error messages. Empty if everything is OK."""
    if pyproj is None:
        return ["The required package 'pyproj' is not installed. It converts the BAG coordinates "
                "(Rijksdriehoekscoordinaten) to WGS84. Install it with: pip install -r requirements.txt"]

    if not os.path.isfile(GRID_FILE):
        return [f"The coordinate transformation grid file '{GRID_FILE}' is missing. It is part of the BAG parser. "
                "Restore it with: git checkout proj/nl_nsgi_rdtrans2018.tif "
                "or download the BAG parser again from https://github.com/digitaldutch/BAG_parser"]

    try:
        for x, y, lat_expected, lon_expected in REFERENCE_POINTS:
            lat, lon = rd_to_wgs84(x, y)
            error_meters = distance_meters(lat, lon, lat_expected, lon_expected)
            if error_meters > MAX_ERROR_METERS:
                return [f"The RD to WGS84 test conversion of RD {x}, {y} is off by {error_meters:.2f} meter. "
                        "PROJ probably did not use the grid file in the 'proj' folder, "
                        "which is needed for accurate coordinates. Check the pyproj installation "
                        "(pip install --upgrade pyproj)."]
    except Exception as e:
        return [f"The RD to WGS84 test conversion failed: {e}"]

    return []

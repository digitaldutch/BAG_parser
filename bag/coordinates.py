# Conversion of Rijksdriehoekscoordinaten (RD, EPSG:28992) to latitude and longitude in ETRS89 (EPSG:4258).
#
# The conversion uses the official transformation RDNAPTRANS2018 of NSGI, using the PROJ library (pyproj) and the
# correction grid in the 'proj' folder. This is accurate to a few centimeters.
#
# ETRS89 is the European reference system that RD is tied to. The latitude and longitude are used as WGS84 without a
# further transformation. The difference between ETRS89 and WGS84 is about 88 cm (1 January 2025) and grows about
# 2.4 cm per year. See Limitations and notes in README.md.

import math
import os

try:
    import pyproj
except ImportError:
    pyproj = None

PROJ_FOLDER = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'proj')
GRID_FILE = os.path.join(PROJ_FOLDER, 'nl_nsgi_rdtrans2018.tif')

# Reference points to test the conversion: RD x, RD y, latitude, longitude (ETRS89, in degrees).
# These are official GNSS stations of the Kadaster, spread over the Netherlands. The coordinates are from RDinfo, the open
# dataset of the Kadaster (license PDM 1.0): https://api.pdok.nl/kadaster/rdinfo/ogc/v1
# The latitude and longitude are converted from degrees, minutes and seconds.
# Without the grid file PROJ uses a less accurate transformation which is off by several decimeters.
REFERENCE_POINTS = [
    (110239.7287, 551803.3498, 52.951409406, 4.721220669),  # Den Helder: RDinfo punt 315 station 11 (2024)
    (236929.9634, 580501.4736, 53.204876725, 6.613405181),  # Groningen: RDinfo punt 346 station 17 (2023)
    (256636.3556, 463590.2701, 52.151125683, 6.872311828),  # Enschede: RDinfo punt 204 station 13 (2021)
    (177216.6770, 317268.6674, 50.844829067, 5.702620878),  # Maastricht: RDinfo punt 327 station 11 (2022)
    (30569.5260, 385280.5456, 51.442875072, 3.597324647),  # Vlissingen: RDinfo punt 334 station 02 (2022)
    (148430.6776, 459486.2470, 52.123552300, 5.291276328),  # Amersfoort: RDinfo punt 357 station 14 (2026)
    (117449.3605, 488971.6223, 52.387314453, 4.835616686),  # Amsterdam: RDinfo punt 376 station 12 (2022)
    (91661.2761, 434821.1809, 51.898295550, 4.466935581),  # Rotterdam: RDinfo punt 531 station 14 (2025)
    (160465.5060, 382722.8813, 51.433585564, 5.465797167),  # Eindhoven: RDinfo punt 336 station 11 (2022)
    (200215.5575, 502469.5553, 52.508039967, 6.053200825),  # Zwolle: RDinfo punt 335 station 14 (2025)
]
MAX_ERROR_METERS = 0.01

_transformer = None


def _get_transformer():
    global _transformer
    if _transformer is None:
        # Make PROJ find the grid file. This is done once in every (multiprocessing) process.
        pyproj.datadir.append_data_dir(PROJ_FOLDER)
        _transformer = pyproj.Transformer.from_crs('EPSG:28992', 'EPSG:4258', always_xy=True)
    return _transformer


def rd_to_wgs84(x, y):
    """Convert RD coordinates to latitude and longitude (ETRS89, used as WGS84). Returns (latitude, longitude)."""
    lon, lat = _get_transformer().transform(x, y)
    return lat, lon


def distance_meters(lat1, lon1, lat2, lon2):
    """Approximate distance in meters between two nearby latitude/longitude points. Accurate enough for short distances."""
    return math.hypot((lat1 - lat2) * 111_200, (lon1 - lon2) * 111_200 * math.cos(math.radians(lat2)))


def check_requirements():
    """Check if the coordinate conversion works. Returns a list with error messages. Empty if everything is OK."""
    if pyproj is None:
        return ["The required package 'pyproj' is not installed. It converts the BAG coordinates "
                "(Rijksdriehoekscoordinaten) to latitude and longitude. Install it with: pip install -r requirements.txt"]

    if not os.path.isfile(GRID_FILE):
        return [f"The coordinate transformation grid file '{GRID_FILE}' is missing. It is part of the BAG parser. "
                "Restore it with: git checkout proj/nl_nsgi_rdtrans2018.tif "
                "or download the BAG parser again from https://github.com/digitaldutch/BAG_parser"]

    try:
        for x, y, lat_expected, lon_expected in REFERENCE_POINTS:
            lat, lon = rd_to_wgs84(x, y)
            error_meters = distance_meters(lat, lon, lat_expected, lon_expected)
            if error_meters > MAX_ERROR_METERS:
                return [f"The test conversion of RD {x}, {y} to latitude and longitude is off by {error_meters:.2f} meter. "
                        "Probably PROJ did not use the grid file in the 'proj' folder, which is needed for accurate "
                        "coordinates. Check the pyproj installation (pip install --upgrade pyproj). "
                        "It is also possible that the reference points in bag/coordinates.py are outdated. "
                        "Run validate_coordinates.py to check this."]
    except Exception as e:
        return [f"The test conversion of RD to latitude and longitude failed: {e}"]

    return []

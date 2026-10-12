# Conversion of Rijksdriehoekscoordinaten (RD, EPSG:28992) to latitude and longitude.
#
# Step 1: RD to ETRS89 (EPSG:4258).
# * With pyproj (recommended): the official transformation RDNAPTRANS2018 of NSGI, using the PROJ library and the correction
#   grid in the 'proj' folder. This is accurate to a fraction of a millimeter.
# * Without pyproj: approximation equations (bag/rijksdriehoek.py) with a constant correction. This is accurate to about
#   7 cm on average and 25 cm at most.
#
# Step 2, only if coordinate_system = 'WGS84' in config.py (needs pyproj): ETRS89 to WGS84 with a time-dependent
# transformation for an epoch (date). ETRS89 is fixed to the Eurasian plate and WGS84 to the globe. The difference is
# about 88 cm (1 January 2025) and grows about 2.4 cm per year. By default step 2 is skipped and the ETRS89 latitude and
# longitude are used as WGS84, as PDOK does. See the Coordinates section in README.md.

import math
import os
from datetime import datetime

import config
from bag import rijksdriehoek

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

ETRS89 = 'ETRS89'
WGS84 = 'WGS84'
# The epoch is stored in an environment variable, so that all (multiprocessing) processes use the same epoch
EPOCH_ENVIRONMENT_VARIABLE = 'BAG_PARSER_WGS84_EPOCH'
# Yearly growth of the difference between ETRS89 and WGS84 in meters. Used to check the transformation.
WGS84_SHIFT_PER_YEAR = 0.0245

# Conversion methods
RDNAPTRANS = 'RDNAPTRANS2018'
APPROXIMATION = 'approximation'
# The approximation equations are about 28 cm off. This constant correction (in degrees, about 21 cm north and 18 cm east)
# makes them accurate to about 7 cm on average and 23 cm at most. It is the median difference between the equations and
# the 442 official GNSS stations of the Kadaster (RDinfo).
APPROXIMATION_CORRECTION_LAT = -0.000001890
APPROXIMATION_CORRECTION_LON = -0.000002687
APPROXIMATION_MAX_ERROR_METERS = 0.30

_transformer = None
_wgs84_transformer = None


def _get_transformer():
    global _transformer
    if _transformer is None:
        # Make PROJ find the grid file. This is done once in every (multiprocessing) process.
        pyproj.datadir.append_data_dir(PROJ_FOLDER)
        _transformer = pyproj.Transformer.from_crs('EPSG:28992', 'EPSG:4258', always_xy=True)
    return _transformer


def _get_wgs84_transformer():
    global _wgs84_transformer
    if _wgs84_transformer is None:
        # Time-dependent transformation from ETRF2000 (ETRS89) to WGS 84 (G2139)
        _wgs84_transformer = pyproj.Transformer.from_crs('EPSG:9067', 'EPSG:9057', always_xy=True)
    return _wgs84_transformer


def decimal_year(moment):
    """Decimal year of a date and time. For example 2026.5 is about 1 July 2026."""
    year_start = datetime(moment.year, 1, 1)
    next_year_start = datetime(moment.year + 1, 1, 1)
    return moment.year + (moment - year_start) / (next_year_start - year_start)


def epoch_to_date(epoch):
    year = int(epoch)
    year_start = datetime(year, 1, 1)
    next_year_start = datetime(year + 1, 1, 1)
    return (year_start + (next_year_start - year_start) * (epoch - year)).date()


def parse_epoch(value):
    """Epoch as decimal year. Accepts a number (2026.5) or a date as text ('2026-06-30')."""
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    try:
        return float(text)
    except ValueError:
        return decimal_year(datetime.strptime(text, '%Y-%m-%d'))


def get_coordinate_system():
    return str(config.coordinate_system).strip().upper()


def get_epoch():
    """Epoch (decimal year) of the WGS84 coordinates. Today if wgs84_epoch is None in config.py."""
    if config.wgs84_epoch is not None:
        return parse_epoch(config.wgs84_epoch)

    value = os.environ.get(EPOCH_ENVIRONMENT_VARIABLE)
    if value is None:
        value = f"{decimal_year(datetime.now()):.3f}"
        os.environ[EPOCH_ENVIRONMENT_VARIABLE] = value
    return float(value)


def describe(coordinate_system=None, epoch=None):
    """Text for the log that tells which coordinate system is used."""
    coordinate_system = coordinate_system or get_coordinate_system()
    if coordinate_system == WGS84:
        epoch = epoch if epoch is not None else get_epoch()
        return f"WGS84 for epoch {epoch:.3f} ({epoch_to_date(epoch)})"
    return "ETRS89 (EPSG:4258)"


def get_method():
    """The conversion method that is used: RDNAPTRANS2018 with pyproj, or the approximation equations without pyproj."""
    return RDNAPTRANS if pyproj is not None else APPROXIMATION


def describe_method(method=None):
    """Text for the log that tells how the RD coordinates are converted."""
    if (method or get_method()) == APPROXIMATION:
        return "approximation equations (accurate to about 7 cm on average, 25 cm at most)"
    return "RDNAPTRANS2018 with pyproj"


def rd_to_etrs89(x, y, method=None):
    """Convert RD coordinates to latitude and longitude in ETRS89. Returns (latitude, longitude)."""
    if (method or get_method()) == APPROXIMATION:
        lat, lon = rijksdriehoek.rijksdriehoek_to_wgs84(x, y)
        return lat + APPROXIMATION_CORRECTION_LAT, lon + APPROXIMATION_CORRECTION_LON

    lon, lat = _get_transformer().transform(x, y)
    return lat, lon


def rd_to_wgs84(x, y, coordinate_system=None, epoch=None, method=None):
    """Convert RD coordinates to latitude and longitude. Returns (latitude, longitude).
    This is ETRS89 (used as WGS84), or real WGS84 for an epoch if coordinate_system is 'WGS84' in config.py."""
    lat, lon = rd_to_etrs89(x, y, method)

    coordinate_system = coordinate_system or get_coordinate_system()
    if coordinate_system == WGS84:
        if pyproj is None:
            raise RuntimeError("The WGS84 coordinates need the package 'pyproj'")
        epoch = epoch if epoch is not None else get_epoch()
        # The height is not known. It has hardly any effect on the latitude and longitude.
        lon, lat, _, _ = _get_wgs84_transformer().transform(lon, lat, 0.0, epoch)

    return lat, lon


def distance_meters(lat1, lon1, lat2, lon2):
    """Approximate distance in meters between two nearby latitude/longitude points. Accurate enough for short distances."""
    return math.hypot((lat1 - lat2) * 111_200, (lon1 - lon2) * 111_200 * math.cos(math.radians(lat2)))


def get_warnings():
    """Warnings for the log. The parser works, but not with the best settings."""
    if pyproj is None:
        return ["The recommended package 'pyproj' is not installed. The approximation equations are used for the coordinates. "
                "They are accurate to about 7 cm on average and 25 cm at most. Install pyproj for the official "
                "transformation, which is accurate to a fraction of a millimeter: pip install -r requirements.txt"]
    return []


def _check_reference_points(method, max_error_meters):
    for x, y, lat_expected, lon_expected in REFERENCE_POINTS:
        lat, lon = rd_to_etrs89(x, y, method)
        error_meters = distance_meters(lat, lon, lat_expected, lon_expected)
        if error_meters > max_error_meters:
            if method == APPROXIMATION:
                return [f"The test conversion of RD {x}, {y} to latitude and longitude with the approximation equations is off "
                        f"by {error_meters:.2f} meter. Expected is at most {max_error_meters:.2f} meter. "
                        "Check bag/rijksdriehoek.py and the correction in bag/coordinates.py."]
            return [f"The test conversion of RD {x}, {y} to latitude and longitude is off by {error_meters:.2f} meter. "
                    "Probably PROJ did not use the grid file in the 'proj' folder, which is needed for accurate "
                    "coordinates. Check the pyproj installation (pip install --upgrade pyproj). "
                    "It is also possible that the reference points in bag/coordinates.py are outdated. "
                    "Run validate_coordinates.py to check this."]
    return []


def check_requirements():
    """Check if the coordinate conversion works. Returns a list with error messages. Empty if everything is OK."""
    coordinate_system = get_coordinate_system()
    if coordinate_system not in (ETRS89, WGS84):
        return [f"config.py: coordinate_system must be 'ETRS89' or 'WGS84', not '{config.coordinate_system}'."]

    if pyproj is None:
        if coordinate_system == WGS84:
            return ["config.py: coordinate_system = 'WGS84' needs the package 'pyproj'. Install it with: "
                    "pip install -r requirements.txt, or use coordinate_system = 'ETRS89'."]
        try:
            return _check_reference_points(APPROXIMATION, APPROXIMATION_MAX_ERROR_METERS)
        except Exception as e:
            return [f"The test conversion of RD to latitude and longitude failed: {e}"]

    if not os.path.isfile(GRID_FILE):
        return [f"The coordinate transformation grid file '{GRID_FILE}' is missing. It is part of the BAG parser. "
                "Restore it with: git checkout proj/nl_nsgi_rdtrans2018.tif "
                "or download the BAG parser again from https://github.com/digitaldutch/BAG_parser"]

    epoch = None
    if coordinate_system == WGS84:
        try:
            epoch = get_epoch()
        except ValueError:
            return [f"config.py: wgs84_epoch must be None, a year like 2026.5 or a date like '2026-06-30', "
                    f"not '{config.wgs84_epoch}'."]

    try:
        errors = _check_reference_points(RDNAPTRANS, MAX_ERROR_METERS)
        if errors:
            return errors

        if coordinate_system == WGS84:
            x, y = REFERENCE_POINTS[0][:2]
            lat, lon = rd_to_etrs89(x, y)
            lat_wgs, lon_wgs = rd_to_wgs84(x, y, WGS84, epoch)
            shift = distance_meters(lat_wgs, lon_wgs, lat, lon)
            expected_shift = WGS84_SHIFT_PER_YEAR * (epoch - 1989)
            if abs(shift - expected_shift) > 0.15:
                return [f"The ETRS89 to WGS84 transformation for epoch {epoch:.3f} gives a shift of {shift:.2f} meter. "
                        f"About {expected_shift:.2f} meter is expected. Check the pyproj installation "
                        "(pip install --upgrade pyproj)."]
    except Exception as e:
        return [f"The test conversion of RD to latitude and longitude failed: {e}"]

    return []

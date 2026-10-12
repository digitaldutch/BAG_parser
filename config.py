import importlib.util
import locale
import os

version = 110
version_date = '12 October 2026'

# At startup the parser checks if a newer version is available on GitHub.
check_for_updates = True

# Change this url if you use a fork.
update_check_url = 'https://raw.githubusercontent.com/digitaldutch/BAG_parser/master/config.py'

# Coordinate system of the latitude and longitude fields (and the geometry):
# 'ETRS89' (default): The European standard that the Rijksdriehoekscoordinaten are tied to. These coordinates never change.
#                     They can be used as WGS84, see the Coordinates section in README.md.
# 'WGS84':            Exact WGS84 coordinates for the epoch below. ETRS89 and WGS84 are about 0.9 meter apart in the
#                     Netherlands and the difference grows about 2.4 cm per year, so WGS84 coordinates depend on the epoch.
coordinate_system = 'ETRS89'

# Epoch (date) for the WGS84 coordinates, if coordinate_system is 'WGS84'. Use a decimal year (2026.5) or a date ('2026-06-30').
# None means today (the date of the import). The epoch is shown in the log and saved in the bag_info table.
# Use a fixed epoch if you want the same coordinates every time. Do not mix databases with different epochs if
# centimeters matter.
wgs84_epoch = None

# Dutch number formatting (e.g. 1.234.567) in log messages. Falls back to the system default if the locale is not installed.
try:
    locale.setlocale(locale.LC_ALL, 'nl_NL')
except locale.Error:
    locale.setlocale(locale.LC_ALL, '')

# Location of the BAG zip file downloaded from kadaster. See README.md
file_bag = 'input/bag.zip'

# Location of the gemeenten file downloaded from cbs.nl. See README.md
file_gemeenten = 'input/gemeenten.csv'

# Output SQLite database with parsed BAG
file_db_sqlite = 'output/bag.sqlite'

# Log-file containing progress, warnings and error messages. This info is also written to the console.
file_log = 'output/bag_importer.log'

# The parser creates an 'adressen' table merging the data of nummers, panden, verblijfsobjecten, ligplaatsen and
# standplaatsen tables into one single table. It only contains active addresses.
create_adressen_table = True

# Only add active records. Historical data of no longer active records are removed.
# The 'adressen' table can only be created if set to True.
active_only = True

# If an adressen table is created some BAG tables are no longer needed and can be deleted:
# nummers, panden, verblijfsobjecten, ligplaatsen and standplaatsen. Set to False if you want to keep these tables.
# You can also delete these tables afterward using the utils_sqlite_shrink.py script.
delete_no_longer_needed_bag_tables = True

# Public spaces with names longer than 24 characters also have a shortened name. Set to true to make short names the
# default if available.
# https://imbag.github.io/praktijkhandleiding/artikelen/hoe-wordt-de-verkorte-schrijfwijze-van-een-openbare-ruimte-bepaald
use_short_street_names = False

# Enable if you want to parse geometry data for woonplaatsen, panden, ligplaatsen and standplaatsen.
# The data is stored in polygon geojson format in the geometry field.
# And the database size will increase from 1.7GB to 16GB. Or 7GB with delete_no_longer_needed_bag_tables enabled.
# Parsing will also take a few minutes more.
parse_geometries = False

# The BAG sometimes contains addresses without a valid public space id. Generally those are invalid addresses.
# They will be automatically deleted if the total number of invalid addresses is lower than the number below.
# Set to 0 if you prefer warning messages and manually check and correct these entries yourself.
delete_addresses_without_public_spaces_if_less_than = 100

# The parser uses multiprocessing to speed up parsing the data. The number of physical CPU cores is a good choice.
# Using logical cores hardly makes a difference. The psutil module automatically determines the physical CPU core count.
# If it is not installed, the number of logical CPU cores is used (os.cpu_count).
# You can also set cpu_cores_used yourself below, after these lines.

has_psutil = importlib.util.find_spec("psutil") is not None
if has_psutil:
    import psutil
    cpu_cores_used = psutil.cpu_count(False)
else:
    # os.cpu_count() returns None if the number can't be determined
    cpu_cores_used = os.cpu_count() or 8

# Windows can't start more than 61 processes in a process pool
if os.name == 'nt':
    cpu_cores_used = min(cpu_cores_used, 61)



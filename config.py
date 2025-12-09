import importlib.util
import locale

version = 92
version_dat = '9 December 2025'

locale.setlocale(locale.LC_ALL, 'nl_NL')

# Location of the BAG zip file downloaded from kadaster. See readme.MD
file_bag = 'input/bag.zip'

# Location of the gemeenten file downloaded from cbs.nl. See readme.MD
file_gemeenten = 'input/gemeenten.csv'

# Output SQLite database with parsed BAG
file_db_sqlite = 'output/bag.sqlite'

# Log file containing progress, warnings and error messages. This info is also written to the console.
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

# The parser uses multiprocessing to speed up parsing the data. For best performance set to the number of
# physical (not logical) CPU cores in your system. Python multiprocessing does not use hyper-threading.
# The psutil module automatically determines the physical CPU core count. If it is not installed,
# it defaults to the manual specified value (8).

has_psutil = importlib.util.find_spec("psutil") is not None
if has_psutil:
    import psutil
    cpu_cores_used = psutil.cpu_count(False)
else:
    cpu_cores_used = 8



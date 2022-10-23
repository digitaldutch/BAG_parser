import locale

version = 49
version_date = '23 october 2022'

locale.setlocale(locale.LC_ALL, 'nl_NL')

# Location of BAG zip file downloaded from kadaster. See readme.MD
file_bag = 'input/bag.zip'

# Location of gemeenten file downloaded from cbs.nl. See readme.MD
file_gemeenten = 'input/gemeenten.csv'

# output SQLite database with parsed BAG
file_db_sqlite = 'output/bag.sqlite'

# log file with progress, warnings and error messages. This info is also written to the console.
file_log = 'output/bag_importer.log'

# The parser creates an 'adressen' table merging the data of nummers, panden, verblijfsobjecten, ligplaatsen and
# standplaatsen tables into one single table
create_adressen_table = True

# If an adressen table is created some BAG tables are no longer needed and can be deleted:
# nummers, panden, verblijfsobjecten, ligplaatsen and standplaatsen. Set to False if you want to keep these tables.
# You can also delete these tables afterwards using the utils_sqlite_shrink.py script.
delete_no_longer_needed_bag_tables = True

# Public spaces with names longer than 24 characters also have a shortened name. Set to true to make short names the
# default if available.
# https://imbag.github.io/praktijkhandleiding/artikelen/hoe-wordt-de-verkorte-schrijfwijze-van-een-openbare-ruimte-bepaald
use_short_street_names = False

# Enable if you want to parse geometry data for woonplaatsen, panden, ligplaatsen and standplaatsen.
# The data is stored in polygon geojson format in the geometry field.
# Parsing with geometries increases the run time from 1 to 2 hours.
# And the database size will increase from 1.7GB to 16GB or 9GB if no longer needed tables are deleted.
parse_geometries = False

# Sometimes the BAG contains addresses without a valid public space id. Generally those are invalid addresses.
# They will be automatically deleted if the total number of invalid addresses is less than the number below.
# Set to 0 if you prefer warning messages and manually check and correct these entries yourself.
delete_addresses_without_public_spaces_if_less_than = 10

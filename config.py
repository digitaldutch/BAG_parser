import locale

version = 26
version_date = '3 augustus 2022'

locale.setlocale(locale.LC_ALL, 'nl_NL')

# Location of BAG zip file downloaded from kadaster. See readme.MD for instructions.
file_bag = 'input/bag.zip'

# Location of gemeenten file downloaded from cbs.nl. See readme.MD for instructions.
file_gemeenten = 'input/gemeenten.csv'

# output SQLite database with parsed BAG
file_db_sqlite = 'output/bag.sqlite'

# log file with progress, warnings and error messages. This info is also written to the console
file_log = 'output/bag_importer.log'

# The parser creates an 'adressen' table. After that some BAG tables are no longer needed and will be deleted:
# nummers, panden, verblijfobjecten and ligplaatsen. Set to False if you want to keep these tables.
# You can also delete these afterwards using the utils_sqlite_shrink.py script.
delete_no_longer_needed_bag_tables = True


import locale

version = 21
version_date = '2 augustus 2022'

locale.setlocale(locale.LC_ALL, 'nl_NL')

# BAG download:
# https://www.kadaster.nl/zakelijk/producten/adressen-en-gebouwen/bag-2.0-extract
# Save the file as 'bag.zip' in the input subfolder

# Gemeenten/provincies table download:
# The latest table is already included in git, but you can upgrade yourself by downloading it from the CDB website.
# https://www.cbs.nl/nl-nl/onze-diensten/methoden/classificaties/overig/gemeentelijke-indelingen-per-jaar
# Save the file as 'gemeenten.csv' in the input subfolder

# BAG zip file downloaded from kadaster
file_bag = 'input/bag.zip'

# gemeenten file downloaded from cbs.nl
file_gemeenten = 'input/gemeenten.csv'

# output SQLite database with parsed BAG
file_db_sqlite = 'output/bag.sqlite'

# log file with progress, warnings and error messages. This info is also written to the console
file_log = 'output/bag_importer.log'

# The parser creates an addresses table. After that some BAG tables are no longer needed: nummers, panden,
# verblijfobjecten and ligplaatsen. You can also delete these afterwards using the utils_sqlite_shrink.py script.
delete_no_longer_needed_bag_tables = False


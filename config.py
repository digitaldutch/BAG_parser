import locale
import config_secret  # Important: You need to create this file yourself. See below.

version = 21
version_date = '2 augustus 2022'

locale.setlocale(locale.LC_ALL, 'nl_NL')

# BAG download:
# https://www.kadaster.nl/zakelijk/producten/adressen-en-gebouwen/bag-2.0-extract
# Save the file in the input subfolder and set the filename in config_secret.py

# Gemeenten/provincies table download:
# The latest table is already included in git, but you can upgrade yourself by downloading it from the CDB website.
# https://www.cbs.nl/nl-nl/onze-diensten/methoden/classificaties/overig/gemeentelijke-indelingen-per-jaar
# Save it in the input subfolder and set the filename in config_secret.py

# Local configurations are done in config_secret.py
# - Local configs are not overwritten when copying the script to another computer.
# - Local configuration info is not stored in source code repository, like git.

# Create a new file named config_secret.php, exclude it from git.
# Copy the code below and save it as config_secret.py.
# ----------
# file_bag = 'input/downloaded_bag_file.zip'
# file_gemeenten = 'input/Gemeenten alfabetisch 2022 - 24 maart.csv'
# file_db_sqlite = 'output/bag.sqlite'
# file_log = 'output/bag_importer.log'
# ----------

# Settings are read from config_secret.py file
file_bag = config_secret.file_bag
file_gemeenten = config_secret.file_gemeenten
file_db_sqlite = config_secret.file_db_sqlite
file_log = config_secret.file_log

# The parser creates an addresses table. After that some BAG tables are no longer needed: nummers, panden,
# verblijfobjecten and ligplaatsen. You can also delete these afterwards using the utils_sqlite_shrink.py script.
delete_no_longer_needed_bag_tables = False


import os, sys
import zipfile
import utils
import config
from database_sqlite import DatabaseSqlite
from bag.bag_parser import BagParser
from bag.gemeente_parser import GemeentenParser

utils.print_log(f"start: parse BAG XML '{config.file_bag}' to sqlite database '{config.file_db_sqlite}'")

if not os.path.exists(config.file_bag): sys.exit('BAG file not found. See readme.MD')

# unzip BAG file to temp folder
utils.print_log('unzip BAG file to temp folder')
if not os.path.exists('temp'): os.makedirs('temp')
utils.empty_folder('temp')
with zipfile.ZipFile(config.file_bag, 'r') as file:
    file.extractall('temp')

db_sqlite = DatabaseSqlite()

utils.print_log("create BAG SQLite database structure")
db_sqlite.create_bag_tables()

# parse gemeenten csv
g_parser = GemeentenParser(db_sqlite)
g_parser.parse()

# parse BAG
b_parser = BagParser(db_sqlite)
b_parser.parse('Woonplaats')
b_parser.parse('GemeenteWoonplaatsRelatie')
b_parser.parse('OpenbareRuimte')
b_parser.parse('Nummeraanduiding')
b_parser.parse('Pand')
b_parser.parse('Verblijfsobject')
b_parser.parse('Ligplaats')
b_parser.parse('Standplaats')

utils.print_log('create indices')
db_sqlite.create_indices_bag()

utils.print_log('create adressen tabel')
db_sqlite.create_adressen_from_bag()
db_sqlite.adressen_fix_bag_errors()
db_sqlite.test_adressen_tabel()

if config.delete_no_longer_needed_bag_tables:
    utils.print_log('delete no longer needed BAG tables')
    db_sqlite.delete_no_longer_needed_bag_tables()

utils.print_log('cleaning up: vacuum')
db_sqlite.commit()
db_sqlite.vacuum()

utils.empty_folder('temp')

db_sqlite.close()

utils.print_log(f'ready: BAG XML to sqlite database {config.file_db_sqlite}')


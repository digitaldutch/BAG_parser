import zipfile
import utils
import config
from database_sqlite import DatabaseSqlite
from bag.bag_parser import BagParser
from bag.gemeente_parser import GemeentenParser

utils.print_log('BAG import into sqlite: start')

# unzip BAG file to temp folder
utils.print_log('unzip BAG to temp folder: ' + config.file_bag)
utils.empty_folder('temp')
with zipfile.ZipFile(config.file_bag, 'r') as file:
    file.extractall('temp')

db_sqlite = DatabaseSqlite()

utils.print_log("create BAG database")
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

utils.print_log('create indices')
db_sqlite.create_indices_bag()

utils.print_log('create adressen tabel')
db_sqlite.create_adressen_from_bag()
db_sqlite.clean_adressen_tabel()
db_sqlite.test_adressen_tabel()

if config.delete_no_longer_needed_bag_tables:
    utils.print_log('delete no longer needed BAG tables')
    db_sqlite.delete_no_longer_needed_bag_tables()

utils.print_log('cleaning up: vacuum')
db_sqlite.commit()
db_sqlite.vacuum()

utils.empty_folder('temp')

db_sqlite.close()

utils.print_log('BAG import into sqlite: ready')

import config
import utils
from database_sqlite import DatabaseSqlite

db_sqlite = DatabaseSqlite()

if config.create_adressen_table and config.delete_no_longer_needed_bag_tables:
    utils.print_log('Delete full BAG tables')
    db_sqlite.delete_no_longer_needed_bag_tables()

utils.print_log('cleaning up: vacuum')
db_sqlite.commit()
db_sqlite.vacuum()

utils.print_log('ready')

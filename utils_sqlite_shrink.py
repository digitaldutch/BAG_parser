import utils
from database_duckdb import DatabaseDuckdb

db_duckdb = DatabaseDuckdb()

utils.print_log('Delete full BAG tables')
db_duckdb.delete_no_longer_needed_bag_tables()

utils.print_log('cleaning up: vacuum')
db_duckdb.commit()
db_duckdb.vacuum()

utils.print_log('ready')

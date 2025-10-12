import platform

import config
import utils
from database_sqlite import DatabaseSqlite

utils.clear_log()
utils.print_log(f"Python version {platform.python_version()}")
utils.print_log(f"BAG parser version {config.version} | {config.version_date}")
utils.print_log(f"start: SQLite database test")

db_sqlite = DatabaseSqlite()

db_sqlite.test_bag_adressen()


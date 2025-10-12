import os
import sys
import platform
import time

import utils
import config
from database_sqlite import DatabaseSqlite
from bag.bag_parser import BagParser
from bag.gemeente_parser import GemeentenParser


def main():
    start_time = time.perf_counter()

    utils.clear_log()
    utils.print_log(f"Python version {platform.python_version()}")

    utils.print_log(f"Using {config.cpu_cores_used} CPU cores")
    if not config.has_psutil:
        utils.print_log_err(
            'CPU cores manually set in config.py. Install the psutil package to auto detect the correct amount of CPU cores.')

    utils.print_log(f"BAG parser version {config.version} | {config.version_date}")
    utils.print_log(f"start: parse BAG XML '{config.file_bag}' to sqlite database '{config.file_db_sqlite}'")

    if not os.path.exists(config.file_bag):
        sys.exit('BAG file not found. See readme.MD')

    temp_folder_name = 'temp'

    # unzip BAG file to temp folder
    utils.print_log('unzip BAG file to temp folder')
    if not os.path.exists(temp_folder_name):
        os.makedirs(temp_folder_name)
    utils.empty_folder(temp_folder_name)

    utils.unzip_files_multithreaded(config.file_bag, temp_folder_name)

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

    utils.print_log('create BAG table indices')
    db_sqlite.create_indices_bag()

    b_parser.add_gemeenten_into_woonplaatsen()

    if config.create_adressen_table:
        if not config.active_only:
            utils.print_log_error('addresses table is only created if active_only=True in config')
        else:
            db_sqlite.create_adressen_from_bag()
            db_sqlite.adressen_remove_dummy_values()
            db_sqlite.test_bag_adressen()

            if config.delete_no_longer_needed_bag_tables:
                utils.print_log('delete no longer needed BAG tables')
                db_sqlite.delete_no_longer_needed_bag_tables()

    utils.print_log('cleaning up: vacuum')
    db_sqlite.vacuum()

    utils.empty_folder(temp_folder_name)

    db_sqlite.close()

    utils.print_log(f"ready: BAG XML to sqlite database '{config.file_db_sqlite}'")

    utils.print_log(f"total run time: {utils.time_elapsed(start_time)}")

if __name__ == '__main__':
    main()

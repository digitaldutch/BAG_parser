import os
import sys
import platform
import utils
import config
from database_sqlite import DatabaseSqlite
from bag.bag_parser import BagParser
from bag.gemeente_parser import GemeentenParser


def main():
    utils.clear_log()
    utils.print_log(f"start: parse BAG XML '{config.file_bag}' to sqlite database '{config.file_db_sqlite}'")
    utils.print_log(f"BAG parser version {config.version} | {config.version_date} | {config.cpu_cores_used} CPU cores")
    utils.print_log(f"Python version {platform.python_version()}")

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

    utils.print_log('create indices')
    db_sqlite.create_indices_bag()

    b_parser.add_gemeenten_into_woonplaatsen()

    if config.create_adressen_table:
        if not config.active_only:
            utils.print_log('addresses table is only created if active_only=True in config', True)
        else:
            db_sqlite.create_adressen_from_bag()
            db_sqlite.adressen_fix_bag_errors()
            db_sqlite.test_bag_adressen()

            if config.delete_no_longer_needed_bag_tables:
                utils.print_log('delete no longer needed BAG tables')
                db_sqlite.delete_no_longer_needed_bag_tables()

    utils.print_log('cleaning up: vacuum')
    db_sqlite.vacuum()

    utils.empty_folder(temp_folder_name)

    db_sqlite.close()

    utils.print_log(f"ready: BAG XML to sqlite database '{config.file_db_sqlite}'")


if __name__ == '__main__':
    main()

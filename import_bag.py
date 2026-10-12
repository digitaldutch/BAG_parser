import os
import sys
import platform
import time
from datetime import datetime

import utils
import config
from database_sqlite import DatabaseSqlite
from bag import coordinates
from bag.bag_parser import BagParser
from bag.gemeente_parser import GemeentenParser


def main():
    start_time = time.perf_counter()

    utils.clear_log()
    utils.print_log(f"Python version {platform.python_version()}")

    utils.print_log(f"BAG parser version {config.version} | {config.version_date}")

    if config.check_for_updates:
        latest_version = utils.get_latest_version(config.update_check_url)
        if latest_version is None:
            utils.print_log("Version check: could not check for a new version (GitHub can't be reached)")
        elif latest_version > config.version:
            utils.print_log_warning(f"Version check: a new version of the BAG parser is available: V{latest_version} "
                                    f"(this is V{config.version}). See CHANGELOG.md on GitHub for the changes. "
                                    "Update with: git pull   and then: pip install -r requirements.txt")
        elif latest_version == config.version:
            utils.print_log(f"Version check: you are using the latest version (V{config.version})")
        else:
            utils.print_log(f"Version check: V{config.version} is newer than the latest version on GitHub "
                            f"(V{latest_version})")
    else:
        utils.print_log("Version check: turned off (check_for_updates in config.py)")

    # Required packages
    errors = coordinates.check_requirements()
    if errors:
        for error in errors:
            utils.print_log_error(error)
        sys.exit('Fix the error above and run the BAG parser again. See the Installation section in README.md')

    # Coordinates. In WGS84 mode this also fixes the epoch for all processes.
    utils.print_log(f"coordinates: {coordinates.describe()}")
    if coordinates.get_coordinate_system() == coordinates.WGS84:
        utils.print_log("The WGS84 coordinates depend on this epoch. It is saved in the bag_info table of the database.")

    # Optional packages
    utils.print_log(f"using {config.cpu_cores_used} CPU cores")
    if not config.has_psutil:
        utils.print_log_warning(
            "The optional package 'psutil' is not installed. The number of CPU cores is set manually in config.py. "
            "Install psutil to detect the number of physical CPU cores automatically: pip install psutil")

    utils.print_log(f"start: parse BAG XML '{config.file_bag}' to sqlite database '{config.file_db_sqlite}'")

    if not os.path.exists(config.file_bag):
        sys.exit('BAG file not found. See README.md')

    temp_folder_name = 'temp'

    # unzip the BAG file to the temp folder
    utils.print_log('unzip BAG file to temp folder')
    if not os.path.exists(temp_folder_name):
        os.makedirs(temp_folder_name)
    utils.empty_folder(temp_folder_name)

    utils.unzip_files_multithreaded(config.file_bag, temp_folder_name)

    db_sqlite = DatabaseSqlite()

    utils.print_log("create BAG SQLite database structure")
    db_sqlite.create_bag_tables()
    db_sqlite.save_bag_info({
        'parser_version': str(config.version),
        'import_date': datetime.now().strftime('%Y-%m-%d'),
        'coordinate_system': coordinates.get_coordinate_system(),
        'wgs84_epoch': f"{coordinates.get_epoch():.3f}" if coordinates.get_coordinate_system() == coordinates.WGS84 else '',
    })

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

    utils.print_log(f"ready: BAG XML to SQLite database '{config.file_db_sqlite}'")

    db_size_mb = os.path.getsize(config.file_db_sqlite) / (1024 * 1024)
    utils.print_log(f"final SQLite file size: {db_size_mb:.1f} MB")

    utils.print_log(f"total run time: {utils.time_elapsed(start_time)}")

if __name__ == '__main__':
    main()

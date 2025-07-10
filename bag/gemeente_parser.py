# Gemeente parser

import time
import utils
import config
import csv


class GemeentenParser:

    def __init__(self, database):
        self.database = database
        self.start_time = None
        self.elapsed_time = None

    def parse(self):
        utils.print_log('parse gemeenten/provincies csv start')
        self.start_time = time.perf_counter()
        self.database.create_gemeenten_provincies(config.file_gemeenten)
        utils.print_log(f"parse gemeenten/provincies csv ready {utils.time_elapsed(self.start_time)}")

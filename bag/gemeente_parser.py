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

        gemeenten = []
        provincies = []
        with open(config.file_gemeenten, newline='', encoding='utf-8') as csv_file:
            reader = csv.reader(csv_file, delimiter=',')
            line_count = 0
            for row in reader:
                if line_count == 0:
                    # Header row
                    if ((not row[0] == 'Gemeentecode')
                            or (not row[2] == 'Gemeentenaam')
                            or (not row[3] == 'Provinciecode')
                            or (not row[5] == 'Provincienaam')):
                        raise Exception('Invalid gemeenten header')
                else:
                    gemeenten.append((row[0], row[2], row[3]))
                    provincie = (row[3], row[5])
                    if provincie not in provincies:
                        provincies.append(provincie)
                line_count += 1

        self.database.save_gemeenten(gemeenten)
        self.database.save_provincies(provincies)

        utils.print_log(f"parse gemeenten/provincies csv ready {utils.time_elapsed(self.start_time)}")

# Export sqlite BAG to csv or other format

import csv
from statusbar import StatusUpdater
import utils
from database_sqlite import DatabaseSqlite


class Exporter:

    def __init__(self):
        self.database = DatabaseSqlite()
        self.total_adressen = 0

    def __export_to_csv(self, output_filename, headers, sql):
        status = StatusUpdater()

        utils.print_log(f"start: export adressen naar csv")
        self.database.check_valid_database()

        file = open(output_filename, 'w', newline='', encoding='utf-8')
        writer = csv.writer(file)

        total_adressen = self.database.fetchone("SELECT COUNT(*) FROM adressen;")
        utils.print_log(f"Totaal aantal adressen: {total_adressen}")

        status.start(total_adressen)

        writer.writerow(headers)

        count = 0
        self.database.cursor.execute(sql)
        for row in self.database.cursor:
            count += 1
            status.update(count)
            writer.writerow(row)
            # if count > 10000: break  # Debug speedup

        status.ready()
        utils.print_log(f"ready: export naar csv")

    def export_to_csv(self, output_filename):
        headers = ['straat', 'huisnummer', 'toevoeging', 'postcode', 'gemeente', 'woonplaats', 'provincie',
                   'bouwjaar', 'rd_x', 'rd_y', 'latitude', 'longitude', 'vloeroppervlakte', 'gebruiksdoel']

        sql = """
            SELECT
              o.naam                       AS straat,
              a.huisnummer,
              a.huisletter || a.toevoeging AS toevoeging,
              a.postcode,
              g.naam                       AS gemeente,
              w.naam                       AS woonplaats,
              p.naam                       AS provincie,
              a.bouwjaar,
              a.rd_x,
              a.rd_y,
              a.latitude,
              a.longitude,
              a.oppervlakte                AS vloeroppervlakte,
              a.gebruiksdoel
            FROM adressen a
              LEFT JOIN openbare_ruimten o ON a.openbare_ruimte_id = o.id
              LEFT JOIN gemeenten g        ON a.gemeente_id        = g.id
              LEFT JOIN woonplaatsen w     ON a.woonplaats_id      = w.id
              LEFT JOIN provincies p       ON g.provincie_id       = p.id;"""

        self.__export_to_csv(output_filename, headers, sql)

    def export_to_csv_postcode(self, output_filename):
        headers = ['straat', 'huisnummer', 'toevoeging', 'postcode', 'woonplaats']

        sql = """
            SELECT
              o.naam                       AS straat,
              a.huisnummer,
              a.huisletter || a.toevoeging AS toevoeging,
              a.postcode,
              w.naam                       AS woonplaats
            FROM adressen a
              LEFT JOIN openbare_ruimten o ON a.openbare_ruimte_id = o.id
              LEFT JOIN woonplaatsen w     ON a.woonplaats_id      = w.id;"""

        self.__export_to_csv(output_filename, headers, sql)

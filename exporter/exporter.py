# Export sqlite BAG to csv or other format

import csv
from statusbar import StatusUpdater
import utils
# from database_sqlite import DatabaseSqlite
import duckdb

class Exporter:

    def __init__(self):
        self.database = DatabaseSqlite()
        self.total_adressen = 0

    def __export_to_csv(self, output_filename, headers, sql, update_status=True):
        status = StatusUpdater()

        utils.print_log(f"start: export adressen naar csv file '{output_filename}'")

        if not self.database.table_exists('adressen'):
            utils.print_log("SQLite database bevat geen adressen tabel. Importeer BAG eerst.", True)
            quit()

        file = open(output_filename, 'w', newline='', encoding='utf-8')
        writer = csv.writer(file)

        if update_status:
            total_adressen = self.database.fetchone("SELECT COUNT(*) FROM adressen;")
            utils.print_log(f"Totaal aantal adressen: {total_adressen}")
            status.start(total_adressen)

        writer.writerow(headers)

        count = 0
        self.database.cursor.execute(sql)
        for row in self.database.cursor:
            count += 1
            if update_status:
                status.update(count)
            writer.writerow(row)
            # if count > 10000: break  # Debug speedup

        if update_status:
            status.end()
        utils.print_log(f"ready: export naar csv")

    def export_to_csv(self, output_filename):
        headers = ['straat', 'huisnummer', 'toevoeging', 'postcode', 'gemeente', 'woonplaats', 'provincie',
                   'bouwjaar', 'rd_x', 'rd_y', 'latitude', 'longitude', 'vloeroppervlakte', 'gebruiksdoel',
                   'hoofdadres_nummer_id']

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
              a.gebruiksdoel,
              a.hoofd_nummer_id
            FROM adressen a
              LEFT JOIN openbare_ruimten o ON a.openbare_ruimte_id = o.id
              LEFT JOIN gemeenten g        ON a.gemeente_id        = g.id
              LEFT JOIN woonplaatsen w     ON a.woonplaats_id      = w.woonplaats_id
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
              LEFT JOIN woonplaatsen w     ON a.woonplaats_id      = w.woonplaats_id;"""

        self.__export_to_csv(output_filename, headers, sql)

    def export_to_csv_postcode4_stats(self, output_filename):
        headers = ['postcode4', 'center_lat', 'center_lon', 'aantal_adressen', 'woonplaats']

        sql = """
          SELECT
            SUBSTR(a.postcode, 0, 5) AS pc4,
            AVG(a.latitude)          AS center_lat,
            AVG(a.longitude)         AS center_lon,
            COUNT(1)                 AS aantal_adressen,
            w.naam                   AS woonplaats
          FROM adressen a
            LEFT JOIN woonplaatsen w ON a.woonplaats_id = w.woonplaats_id
          WHERE a.postcode <> ''
          GROUP BY pc4;"""

        self.__export_to_csv(output_filename, headers, sql, False)

    def export_to_csv_postcode5_stats(self, output_filename):
        headers = ['postcode5', 'center_lat', 'center_lon', 'aantal_adressen', 'woonplaats']

        sql = """
          SELECT
            SUBSTR(a.postcode, 0, 6) AS pc5,
            AVG(a.latitude)          AS center_lat,
            AVG(a.longitude)         AS center_lon,
            COUNT(1)                 AS aantal_adressen,
            w.naam                   AS woonplaats
          FROM adressen a
            LEFT JOIN woonplaatsen w ON a.woonplaats_id = w.woonplaats_id
          WHERE a.postcode <> ''
          GROUP BY pc5;"""

        self.__export_to_csv(output_filename, headers, sql, False)

    def export_to_csv_postcode6_stats(self, output_filename):
        headers = ['postcode6', 'center_lat', 'center_lon', 'aantal_adressen', 'woonplaats']

        sql = """
          SELECT
            a.postcode       AS pc6,
            AVG(a.latitude)  AS center_lat,
            AVG(a.longitude) AS center_lon,
            COUNT(1)         AS aantal_adressen,
            w.naam           AS woonplaats
          FROM adressen a
            LEFT JOIN woonplaatsen w ON a.woonplaats_id = w.woonplaats_id
          WHERE a.postcode <> ''
          GROUP BY pc6;"""

        self.__export_to_csv(output_filename, headers, sql, False)

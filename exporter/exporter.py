# Export DuckDB BAG to csv or other format

import utils
from database_duckdb import DatabaseDuckdb

class Exporter:

    def __init__(self):
        self.database = DatabaseDuckdb()
        self.total_adressen = 0

    def __export(self, output_filename, export_options, sql):

        utils.print_log(f"start: export adressen naar bestand '{output_filename}'")

        if not self.database.table_exists('adressen'):
            utils.print_log("DuckDB database bevat geen adressen tabel. Importeer BAG eerst.", True)
            quit()

        sqlcmd = f"COPY ({sql}) TO '{output_filename}' {export_options};"
        self.database.connection.execute(sqlcmd)

    def export(self, output_filename, export_options):
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
              LEFT JOIN provincies p       ON g.provincie_id       = p.id"""

        self.__export(output_filename, export_options, sql)

    def export_postcode(self, output_filename, export_options):
        sql = """
            SELECT
              o.naam                       AS straat,
              a.huisnummer,
              a.huisletter || a.toevoeging AS toevoeging,
              a.postcode,
              w.naam                       AS woonplaats
            FROM adressen a
              LEFT JOIN openbare_ruimten o ON a.openbare_ruimte_id = o.id
              LEFT JOIN woonplaatsen w     ON a.woonplaats_id      = w.woonplaats_id"""

        self.__export(output_filename, export_options, sql)

    def export_postcode4_stats(self, output_filename, export_options):
        sql = """
          SELECT
            SUBSTR(a.postcode, 0, 5) AS pc4,
            AVG(a.latitude)          AS center_lat,
            AVG(a.longitude)         AS center_lon,
            COUNT(1)                 AS aantal_adressen,
            FIRST(w.naam)            AS woonplaats
          FROM adressen a
            LEFT JOIN woonplaatsen w ON a.woonplaats_id = w.woonplaats_id
          WHERE a.postcode <> ''
          GROUP BY pc4"""

        self.__export(output_filename, export_options, sql)

    def export_postcode5_stats(self, output_filename, export_options):
        sql = """
          SELECT
            SUBSTR(a.postcode, 0, 6) AS pc5,
            AVG(a.latitude)          AS center_lat,
            AVG(a.longitude)         AS center_lon,
            COUNT(1)                 AS aantal_adressen,
            FIRST(w.naam)            AS woonplaats
          FROM adressen a
            LEFT JOIN woonplaatsen w ON a.woonplaats_id = w.woonplaats_id
          WHERE a.postcode <> ''
          GROUP BY pc5"""

        self.__export(output_filename, export_options, sql)

    def export_postcode6_stats(self, output_filename, export_options):
        sql = """
          SELECT
            a.postcode       AS pc6,
            AVG(a.latitude)  AS center_lat,
            AVG(a.longitude) AS center_lon,
            COUNT(1)         AS aantal_adressen,
            FIRST(w.naam)    AS woonplaats
          FROM adressen a
            LEFT JOIN woonplaatsen w ON a.woonplaats_id = w.woonplaats_id
          WHERE a.postcode <> ''
          GROUP BY pc6"""

        self.__export(output_filename, export_options, sql)

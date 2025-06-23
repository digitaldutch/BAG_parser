import duckdb

import utils
import config
import pandas as pd
import numpy as np


class DatabaseDuckdb:
    connection = None

    # cursor = None

    def __init__(self):
        self.connection = duckdb.connect(config.file_db_duckdb)
        # self.connection = duckdb.connect()

    def close(self):
        self.connection.commit()
        self.connection.close()

    def commit(self):
        self.connection.commit()

    def fetchmany_init(self, sql):
        self.execute(sql)

    def fetchone(self, sql):
        return self.connection.execute(sql).fetchone()[0]

    def fetchall(self, sql):
        return self.connection.execute(sql).fetchall()

    def fetchmany(self, size=1000):
        return self.connection.fetchmany(size)

    def start_transaction(self):
        self.connection.execute("BEGIN TRANSACTION")

    def commit_transaction(self):
        self.connection.execute("COMMIT TRANSACTION")

    def vacuum(self):
        # self.connection.execute("VACUUM")
        None

    def save_woonplaats(self, datarows):
        df = pd.DataFrame(datarows)
        pd.set_option("future.no_silent_downcasting", True)
        df = df.replace(r'^\s*$', np.nan, regex=True)
        self.connection.execute(
            """INSERT INTO woonplaatsen (woonplaats_id, naam, geometry, status, begindatum_geldigheid, einddatum_geldigheid) select 
            id as woonplaatsen_id, naam, geometry, status, 
            begindatum_geldigheid, einddatum_geldigheid
            FROM df"""
                                )
        # sqlvals = ""
        # for data in datarows:
        #     sqlvals += "({},'{}','{}','{}','{}','{}'),".format(data["id"],
        #                                                        str(data["naam"]).replace("'", "''"),
        #                                                        str(data["geometry"]).replace("'", "''"),
        #                                                        str(data["status"]).replace("'", "''"),
        #                                                        data["begindatum_geldigheid"],
        #                                                        data["einddatum_geldigheid"]
        #                                                        )
        # sql = f"""INSERT INTO woonplaatsen (woonplaats_id, naam, geometry, status, begindatum_geldigheid, einddatum_geldigheid)
        #     VALUES {sqlvals}
        #     """
        # with self.open() as connection:
        #     connection.execute(sql)
        #     connection.close()

    def save_woonplaats_geometry(self, woonplaatsen):
        self.connection.executemany(
            "UPDATE woonplaatsen SET geometry=? WHERE id=?;",
            woonplaatsen)

    def save_pand_geometry(self, panden):
        self.connection.executemany(
            "UPDATE panden SET geometry=? WHERE id=?;",
            panden)

    def save_lon_lat(self, table_name, records):
        self.connection.executemany(
            f"UPDATE {table_name} SET longitude=?, latitude=? WHERE id=?;",
            records)

    def save_gemeenten(self, gemeenten):
        self.connection.executemany(
            "INSERT INTO gemeenten (id, naam, provincie_id) VALUES(?, ?, ?);",
            gemeenten)

    def save_gemeente_woonplaats(self, datarows):
        df = pd.DataFrame(datarows)
        pd.set_option("future.no_silent_downcasting", True)
        df = df.replace(r'^\s*$', np.nan, regex=True)
        try:
            self.connection.execute("INSERT INTO gemeente_woonplaatsen SELECT "
                               "gemeente_id,"
                               "woonplaats_id,"
                               "status,"
                               "begindatum_geldigheid,"
                               "einddatum_geldigheid"
                               " FROM df")
        except Exception as e:
            print(e, flush=True)
            # print(df.dtypes, flush=True)

    def add_gemeenten_to_woonplaatsen(self):
        self.connection.execute(
            """UPDATE woonplaatsen SET gemeente_id=gw.gemeente_id
            FROM (SELECT gemeente_id, woonplaats_id FROM gemeente_woonplaatsen) AS gw
            WHERE gw.woonplaats_id = woonplaatsen.woonplaats_id
            """)

    def save_provincies(self, provincies):
        self.connection.executemany(
            "INSERT INTO provincies (id, naam) VALUES(?, ?)",
            provincies)

    def save_openbare_ruimte(self, datarows):
        for data in datarows:
            if config.use_short_street_names:
                data["naam"] = data["verkorte_naam"] if data["verkorte_naam"] != '' else data["lange_naam"]
            else:
                data["naam"] = data["lange_naam"]

        df = pd.DataFrame(datarows)
        pd.set_option("future.no_silent_downcasting", True)
        df = df.replace(r'^\s*$', np.nan, regex=True)
        self.connection.execute("INSERT OR REPLACE INTO openbare_ruimten SELECT "
                           "id, "
                           "naam, "
                           "lange_naam, "
                           "verkorte_naam, "
                           "type, "
                           "woonplaats_id,"
                           "status,"
                           "begindatum_geldigheid,"
                           "einddatum_geldigheid"
                           " FROM df")

    def save_nummer(self, datarows):
        df = pd.DataFrame(datarows)
        pd.set_option("future.no_silent_downcasting", True)
        df = df.replace(r'^\s*$', np.nan, regex=True)
        try:
            self.connection.execute("INSERT OR REPLACE INTO nummers SELECT "
                               "id,postcode,huisnummer,"
                               "huisletter,"
                               "toevoeging,"
                               "woonplaats_id,"
                               "openbare_ruimte_id,"
                               "status,"
                               "begindatum_geldigheid,"
                               "einddatum_geldigheid"
                               " FROM df")
        except Exception as e:
            print(e, flush=True)
            # print(df.dtypes, flush=True)

    def save_pand(self, datarows):
        df = pd.DataFrame(datarows)
        pd.set_option("future.no_silent_downcasting", True)
        df = df.replace(r'^\s*$', np.nan, regex=True)

        # connection = self.open()
        self.connection.execute("INSERT OR REPLACE INTO panden SELECT "
                           "id, bouwjaar, geometry,"
                           "status,"
                           "begindatum_geldigheid,"
                           "einddatum_geldigheid"
                           " FROM df")


    def save_verblijfsobject(self, datarows):
        df = pd.DataFrame(datarows)
        pd.set_option("future.no_silent_downcasting", True)
        df = df.replace(r'^\s*$', np.nan, regex=True)
        try:
            self.connection.execute("INSERT OR REPLACE INTO verblijfsobjecten SELECT "
                               "id,nummer_id,pand_id,"
                               "try_cast(oppervlakte as double) as oppervlakte,"
                               "try_cast(rd_x as double) as rd_x ,"
                               "try_cast(rd_y as double) as rd_y,"
                               "try_cast(latitude as double) as latitude,"
                               "try_cast(longitude as double) as longitude,"
                               "gebruiksdoel,"
                               "nevenadressen,"
                               "status,"
                               "begindatum_geldigheid,"
                               "einddatum_geldigheid"
                               " FROM df")
        except Exception as e:
            print(e, flush=True)
            # print(df.dtypes, flush=True)

    def save_ligplaats(self, datarows):
        df = pd.DataFrame(datarows)
        pd.set_option("future.no_silent_downcasting", True)
        df = df.replace(r'^\s*$', np.nan, regex=True)
        try:
            self.connection.execute("INSERT OR REPLACE INTO ligplaatsen SELECT "
                               "id,nummer_id,"
                               "try_cast(rd_x as double) as rd_x ,"
                               "try_cast(rd_y as double) as rd_y,"
                               "try_cast(latitude as double) as latitude,"
                               "try_cast(longitude as double) as longitude,"
                               "geometry,"
                               "status,"
                               "begindatum_geldigheid,"
                               "einddatum_geldigheid"
                               " FROM df")
        except Exception as e:
            print(e, flush=True)
            # print(df.dtypes, flush=True)

    def save_standplaats(self, datarows):
        df = pd.DataFrame(datarows)
        pd.set_option("future.no_silent_downcasting", True)
        df = df.replace(r'^\s*$', np.nan, regex=True)
        try:
            self.connection.execute("INSERT OR REPLACE INTO standplaatsen SELECT "
                               "id,nummer_id,"
                               "try_cast(rd_x as double) as rd_x ,"
                               "try_cast(rd_y as double) as rd_y,"
                               "try_cast(latitude as double) as latitude,"
                               "try_cast(longitude as double) as longitude,"
                               "geometry,"
                               "status,"
                               "begindatum_geldigheid,"
                               "einddatum_geldigheid"
                               " FROM df")
        except Exception as e:
            print(e, flush=True)
            # print(df.dtypes, flush=True)

    def create_bag_tables(self):
        self.connection.execute("""
            DROP TABLE IF EXISTS gemeenten;
            CREATE TABLE gemeenten (id UBIGINT PRIMARY KEY, naam TEXT, provincie_id UBIGINT);
            
            DROP TABLE IF EXISTS provincies;
            CREATE TABLE provincies (id UBIGINT PRIMARY KEY, naam TEXT);
            
            DROP TABLE IF EXISTS woonplaatsen;
            CREATE OR REPLACE SEQUENCE seq_wpid START 1;
            CREATE TABLE woonplaatsen (id UBIGINT PRIMARY KEY DEFAULT NEXTVAL('seq_wpid'), woonplaats_id UBIGINT, naam TEXT, gemeente_id UBIGINT, geometry TEXT,
                status TEXT, begindatum_geldigheid TEXT, einddatum_geldigheid TEXT);
            
            DROP TABLE IF EXISTS gemeente_woonplaatsen;
            CREATE TABLE gemeente_woonplaatsen (
                gemeente_id UBIGINT,
                woonplaats_id UBIGINT,
                status TEXT, 
                begindatum_geldigheid DATE, 
                einddatum_geldigheid DATE
            );              

            DROP TABLE IF EXISTS openbare_ruimten;
            CREATE TABLE openbare_ruimten (id UBIGINT PRIMARY KEY, naam TEXT, lange_naam TEXT, verkorte_naam TEXT, 
                type TEXT, woonplaats_id UBIGINT, status TEXT, begindatum_geldigheid DATE, einddatum_geldigheid DATE);

            DROP TABLE IF EXISTS nummers;
            CREATE TABLE nummers (id TEXT PRIMARY KEY, 
                postcode TEXT, 
                huisnummer INTEGER, 
                huisletter TEXT,
                toevoeging TEXT, 
                woonplaats_id UBIGINT, 
                openbare_ruimte_id UBIGINT,
                status TEXT, 
                begindatum_geldigheid DATE, 
                einddatum_geldigheid DATE);

            DROP TABLE IF EXISTS panden;
            CREATE TABLE panden (id TEXT PRIMARY KEY, 
                bouwjaar INTEGER, 
                geometry TEXT,
                status TEXT, 
                begindatum_geldigheid DATE, 
                einddatum_geldigheid DATE);

            DROP TABLE IF EXISTS verblijfsobjecten;
            CREATE TABLE verblijfsobjecten (
                id TEXT PRIMARY KEY, 
                nummer_id TEXT, 
                pand_id TEXT, 
                oppervlakte FLOAT, 
                rd_x FLOAT, 
                rd_y FLOAT, 
                latitude FLOAT, 
                longitude FLOAT, 
                gebruiksdoel TEXT, 
                nevenadressen TEXT,
                status TEXT, 
                begindatum_geldigheid DATE, 
                einddatum_geldigheid DATE);           

            DROP TABLE IF EXISTS ligplaatsen;
            CREATE TABLE ligplaatsen (
                id TEXT PRIMARY KEY, 
                nummer_id TEXT, 
                rd_x FLOAT, 
                rd_y FLOAT, 
                latitude FLOAT, 
                longitude FLOAT, 
                geometry TEXT, 
                status TEXT, 
                begindatum_geldigheid DATE, 
                einddatum_geldigheid DATE);              

            DROP TABLE IF EXISTS standplaatsen;
            CREATE TABLE standplaatsen (
                id TEXT PRIMARY KEY, 
                nummer_id TEXT, 
                rd_x FLOAT, 
                rd_y FLOAT, 
                latitude FLOAT, 
                longitude FLOAT, 
                geometry TEXT, 
                status TEXT, 
                begindatum_geldigheid DATE, 
                einddatum_geldigheid DATE);              
        """)

    def create_adressen_from_bag(self):

        utils.print_log('create adressen tabel: import adressen')
        self.connection.execute(f"""
            DROP TABLE IF EXISTS adressen;
            
            CREATE TABLE adressen (
                nummer_id TEXT PRIMARY KEY, 
                nummer_begindatum_geldigheid DATE, 
                nummer_einddatum_geldigheid DATE, 
                pand_id TEXT, 
                pand_begindatum_geldigheid DATE, 
                pand_einddatum_geldigheid DATE, 
                verblijfsobject_id TEXT, 
                gemeente_id UBIGINT, 
                woonplaats_id UBIGINT, 
                openbare_ruimte_id UBIGINT, 
                object_type TEXT, 
                gebruiksdoel TEXT, 
                postcode TEXT, 
                huisnummer INTEGER, 
                huisletter TEXT, 
                toevoeging TEXT, 
                oppervlakte FLOAT, 
                rd_x FLOAT, 
                rd_y FLOAT, 
                latitude FLOAT, 
                longitude FLOAT, 
                bouwjaar INTEGER,
                hoofd_nummer_id TEXT, 
                geometry TEXT);

            INSERT INTO adressen (
                nummer_id, 
                nummer_begindatum_geldigheid, 
                nummer_einddatum_geldigheid, 
                pand_id, 
                pand_begindatum_geldigheid, 
                pand_einddatum_geldigheid, 
                verblijfsobject_id, 
                gemeente_id, 
                woonplaats_id, 
                openbare_ruimte_id, 
                object_type, 
                gebruiksdoel, 
                postcode, 
                huisnummer, 
                huisletter, 
                toevoeging, 
                oppervlakte, 
                rd_x, 
                rd_y, 
                longitude, 
                latitude, 
                bouwjaar, 
                geometry)
            SELECT
                n.id AS nummer_id,
                n.begindatum_geldigheid,
                n.einddatum_geldigheid,
                p.id,
                p.begindatum_geldigheid,
                p.einddatum_geldigheid,
                v.id AS verblijfsobject_id,
                w.gemeente_id,
                o.woonplaats_id,
                o.id,
                'verblijfsobject',
                v.gebruiksdoel,
                n.postcode,
                n.huisnummer,
                n.huisletter,
                n.toevoeging,
                v.oppervlakte,
                v.rd_x,
                v.rd_y,
                v.longitude,
                v.latitude,
                p.bouwjaar,
                p.geometry
            FROM nummers n
            LEFT JOIN openbare_ruimten o  ON o.id            = n.openbare_ruimte_id
            LEFT JOIN woonplaatsen w      ON w.woonplaats_id = o.woonplaats_id
            LEFT JOIN verblijfsobjecten v ON v.nummer_id     = n.id
            LEFT JOIN panden p            ON v.pand_id       = p.id;
        """)

        utils.print_log('create adressen tabel: importeer pand info voor adressen met meerdere panden')
        self.adressen_import_meerdere_panden()

        utils.print_log('create adressen tabel: import ligplaatsen data')
        self.adressen_import_ligplaatsen()

        utils.print_log('create adressen tabel: import standplaatsen data')
        self.adressen_import_standplaatsen()

        utils.print_log('create adressen tabel: import woonplaatsen from nummers')
        self.adressen_update_woonplaatsen_from_nummers()

        # utils.print_log('create adressen tabel: create indices')
        # self.create_indices_adressen()

        utils.print_log('create adressen tabel: update nevenadressen data')
        self.adressen_update_nevenadressen()

        # self.connection.commit()

    def adressen_import_meerdere_panden(self):

        self.connection.execute("""
            DROP TABLE IF EXISTS temp_pand_ids;
            
            CREATE TEMP TABLE temp_pand_ids (
            nummer_id TEXT,
            pand_id TEXT
        );""")

        adressen = self.fetchall(
            "SELECT nummer_id, pand_id FROM verblijfsobjecten WHERE pand_id LIKE '%,%'")

        parameters = []
        for adres in adressen:
            pand_ids = adres[1].split(',')
            for pand_id in pand_ids:
                parameters.append([adres[0], pand_id])

        sql = "INSERT INTO temp_pand_ids (nummer_id, pand_id) VALUES (?, ?)"
        if len(parameters) > 0:
            self.connection.executemany(sql, parameters)

        # Copy bouwjaar and geometry to adressen table. Only last one remains.
        # Maybe add a multi-bouwjaar and multi-geometry option later.
        self.connection.execute("""
            UPDATE adressen SET
              geometry = p.geometry,
              bouwjaar = p.bouwjaar
            FROM (
                   SELECT
                     t.pand_id,
                     t.nummer_id,
                     panden.geometry,
                     panden.bouwjaar
                   FROM temp_pand_ids t
                          LEFT JOIN panden ON panden.id = t.pand_id
                 ) AS p
            WHERE p.nummer_id = adressen.nummer_id;
        """)

    def adressen_import_ligplaatsen(self):
        self.connection.execute("""
            UPDATE adressen SET
              rd_x = l.rd_x,
              rd_y = l.rd_y,
              latitude = l.latitude,
              longitude = l.longitude,
              geometry = l.geometry,
              object_type = 'ligplaats'
            FROM (SELECT rd_x, rd_y, latitude, longitude, geometry, nummer_id FROM ligplaatsen) AS l
            WHERE l.nummer_id = adressen.nummer_id;           
        """)

    def adressen_import_standplaatsen(self):
        self.connection.execute("""
            UPDATE adressen SET
              rd_x = s.rd_x,
              rd_y = s.rd_y,
              latitude = s.latitude,
              longitude = s.longitude,
              geometry = s.geometry,
              object_type = 'standplaats'
            FROM (SELECT rd_x, rd_y, latitude, longitude, geometry, nummer_id FROM standplaatsen) AS s
            WHERE s.nummer_id = adressen.nummer_id;
        """)

    def adressen_update_nevenadressen(self):
        self.connection.execute("""
            DROP TABLE IF EXISTS nevenadressen;
            
            CREATE TEMP TABLE nevenadressen (
            neven_nummer_id TEXT PRIMARY KEY,
            hoofd_nummer_id TEXT
        );""")

        adressen = self.fetchall(
            "SELECT nummer_id, nevenadressen FROM verblijfsobjecten WHERE nevenadressen <> ''")
        parameters = []
        for adres in adressen:
            neven_nummer_ids = adres[1].split(',')
            for neven_id in neven_nummer_ids:
                parameters.append([adres[0], neven_id])

        sql = "INSERT INTO nevenadressen (hoofd_nummer_id, neven_nummer_id) VALUES (?, ?)"
        if len(parameters) > 0:
            self.connection.executemany(sql, parameters)

        self.connection.execute("""
            UPDATE adressen SET
                hoofd_nummer_id = n.hoofd_nummer_id,
                pand_id = n.pand_id,
                verblijfsobject_id = n.verblijfsobject_id,
                gebruiksdoel = n.gebruiksdoel,
                oppervlakte = n.oppervlakte,
                rd_x = n.rd_x,
                rd_y = n.rd_y,
                latitude = n.latitude,
                longitude = n.longitude,
                bouwjaar = n.bouwjaar,
                geometry = n.geometry
            FROM (
                SELECT
                    nevenadressen.hoofd_nummer_id,
                    nevenadressen.neven_nummer_id,
                    adressen.pand_id,
                    adressen.verblijfsobject_id,
                    adressen.gebruiksdoel,
                    adressen.oppervlakte,
                    adressen.rd_x,
                    adressen.rd_y,
                    adressen.latitude,
                    adressen.longitude,
                    adressen.bouwjaar,
                    adressen.geometry
                FROM nevenadressen
                LEFT JOIN adressen ON nevenadressen.hoofd_nummer_id = adressen.nummer_id
                 ) AS n
            WHERE n.neven_nummer_id = adressen.nummer_id;
        """)

    # woonplaats_id in nummers overrule woonplaats_id van de openbare ruimte.
    def adressen_update_woonplaatsen_from_nummers(self):
        self.connection.execute("""
            UPDATE adressen SET
              woonplaats_id = n.woonplaats_id
            FROM (SELECT id, woonplaats_id FROM nummers WHERE woonplaats_id is not NULL) AS n
            WHERE n.id = adressen.nummer_id;
        """)

    def delete_no_longer_needed_bag_tables(self):
        self.connection.execute("""
          DROP TABLE IF EXISTS nummers; 
          DROP TABLE IF EXISTS panden; 
          DROP TABLE IF EXISTS verblijfsobjecten; 
          DROP TABLE IF EXISTS ligplaatsen; 
          DROP TABLE IF EXISTS standplaatsen; 
        """)

    def adressen_remove_dummy_values(self):
        # The BAG contains dummy values in some fields (bouwjaar, oppervlakte)
        # See: https://geoforum.nl/t/zijn-dummy-waarden-in-de-bag-toegestaan/9091/5

        # Amsterdam heeft een reeks van panden met dummy bouwjaar 1005
        # https://www.amsterdam.nl/stelselpedia/bag-index/catalogus-bag/objectklasse-pand/bouwjaar-pand/
        panden = self.fetchall(f"SELECT pand_id, bouwjaar FROM adressen WHERE bouwjaar=1005;")
        aantal = len(panden)
        utils.print_log(f"fix: test adressen met dummy bouwjaar 1005 in Amsterdam: {aantal: n}")
        if aantal > 0:
            utils.print_log(f"fix: verwijder {aantal:n} ongeldige 1005 bouwjaren")
            self.connection.execute(f"UPDATE adressen SET bouwjaar=NULL WHERE bouwjaar=1005;")

        # The BAG contains some buildings with bouwjaar 9999
        last_valid_build_year = 2040
        panden = self.fetchall(
            f"SELECT pand_id, bouwjaar FROM adressen WHERE bouwjaar > {last_valid_build_year}")
        aantal = len(panden)

        # Show max first 10 items with invalid build year
        panden = panden[slice(10)]

        text_panden = ''
        for pand in panden:
            if text_panden:
                text_panden += ','
            text_panden += pand[0] + ' ' + str(pand[1])
        if text_panden:
            text_panden = f" | panden: {text_panden}"

        utils.print_log(
            f"fix: test adressen met ongeldig bouwjaar > {last_valid_build_year}: {aantal: n}{text_panden}")

        if aantal > 0:
            utils.print_log(f"fix: verwijder {aantal:n} ongeldige bouwjaren (> {last_valid_build_year})")
            self.connection.execute(f"UPDATE adressen SET bouwjaar=NULL WHERE bouwjaar > {last_valid_build_year}")

        # The BAG contains some residences with oppervlakte 999999
        verblijfsobject_ids = self.fetchall(
            "SELECT verblijfsobject_id FROM adressen WHERE oppervlakte = 999999;")
        aantal = len(verblijfsobject_ids)

        text_ids = ''
        for verblijfsobject_id in verblijfsobject_ids:
            if text_ids:
                text_ids += ','
            text_ids += verblijfsobject_id[0]
        if text_ids:
            text_ids = f" | verblijfsobject_ids: {text_ids}"

        utils.print_log(f"fix: test adressen met ongeldige oppervlakte = 999999: {aantal: n}{text_ids}")
        if aantal > 0:
            utils.print_log(f"fix: verwijder {aantal:n} ongeldige oppervlaktes (999999)")
            self.connection.execute("UPDATE adressen SET oppervlakte=NULL WHERE oppervlakte = 999999;")

        # The BAG contains some residences with oppervlakte 1 (In Amsterdam this is a valid dummy)
        # https://www.amsterdam.nl/stelselpedia/bag-index/catalogus-bag/objectklasse-vbo/gebruiksoppervlakte/
        verblijfsobject_ids = self.fetchall(
            "SELECT verblijfsobject_id FROM adressen WHERE oppervlakte = 1;")
        aantal = len(verblijfsobject_ids)
        utils.print_log(f"fix: test adressen met ongeldige oppervlakte = 1 (dummy value in Amsterdam): {aantal: n}")
        if aantal > 0:
            utils.print_log(f"fix: verwijder {aantal:n} met ongeldige oppervlakte = 1")
            self.connection.execute("UPDATE adressen SET oppervlakte=NULL WHERE oppervlakte = 1;")

        # The BAG contains some addresses without valid public space
        address_count = self.fetchone(
            "SELECT COUNT(*) FROM adressen WHERE openbare_ruimte_id IS NULL "
            " OR openbare_ruimte_id NOT IN (SELECT id FROM openbare_ruimten);")
        utils.print_log("fix: test adressen zonder openbare ruimte: " + str(address_count))

        # Delete them if not too many
        if (address_count > 0) and (address_count < config.delete_addresses_without_public_spaces_if_less_than):
            utils.print_log(f"fix: verwijder {address_count:n} adressen zonder openbare ruimte")
            self.connection.execute("DELETE FROM adressen WHERE openbare_ruimte_id IS NULL "
                               "OR openbare_ruimte_id NOT IN (SELECT id FROM openbare_ruimten)")


    def table_exists(self, table_name):
        # Check if database contains adressen tabel
        count = self.fetchone(
            f"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '{table_name}';")
        return count == 1

    def test_bag_adressen(self) -> bool:
        """
            Tests the BAG (Basisregistratie Adressen en Gebouwen) data integrity.

            This method checks if there are any errors in the database related to municipalities
            without associated addresses and other possible issues.

            Returns:
                bool: True if no errors were found (total_error_count == 0),
                      False otherwise.
            """
        total_error_count = 0

        if not self.table_exists('adressen'):
            utils.print_log("SQLite database bevat geen adressen tabel. Importeer BAG eerst.", True)
            quit()

        utils.print_log(f"start: tests op BAG SQLite database: '{config.file_db_duckdb}'")

        sql = "SELECT nummer_begindatum_geldigheid FROM adressen ORDER BY nummer_begindatum_geldigheid DESC LIMIT 1"
        datum = self.fetchone(sql)
        utils.print_log(f"info: laatste nummer_begindatum_geldigheid: {datum}")

        sql = "SELECT pand_begindatum_geldigheid FROM adressen ORDER BY pand_begindatum_geldigheid DESC LIMIT 1"
        datum = self.fetchone(sql)
        utils.print_log(f"info: laatste pand_begindatum_geldigheid: {datum}")

        # Soms zitten er nog oude gemeenten die niet meer bestaan in de gemeenten.csv filee
        count = self.fetchone("""
            SELECT COUNT(*) FROM gemeenten
            WHERE id NOT IN (SELECT DISTINCT gemeente_id FROM adressen);
            """)
        total_error_count += count
        utils.print_log("test: gemeenten zonder adressen: " + str(count), count > 0)

        if count > 0:
            gemeenten = self.fetchall("""
                SELECT id, naam FROM gemeenten 
                WHERE id NOT IN (SELECT DISTINCT gemeente_id FROM adressen);
                """)

            gemeenten_formatted = ', '.join(f"{gemeente[0]} {gemeente[1]}" for gemeente in gemeenten)

            utils.print_log("test: gemeenten zonder adressen: " + gemeenten_formatted, count > 0)

        count = self.fetchone("""
            SELECT COUNT(*) FROM woonplaatsen 
            WHERE gemeente_id IS NULL OR gemeente_id NOT IN (SELECT id FROM gemeenten);
            """)
        total_error_count += count
        utils.print_log("test: woonplaatsen zonder gemeente: " + str(count), count > 0)

        count = self.fetchone("""
            SELECT COUNT(*) FROM adressen 
            WHERE openbare_ruimte_id IS NULL
                OR openbare_ruimte_id NOT IN (SELECT id FROM openbare_ruimten)
            """)
        total_error_count += count
        utils.print_log("test: adressen zonder openbare ruimte: " + str(count), count > 0)

        count = self.fetchone("SELECT COUNT(*) FROM adressen WHERE woonplaats_id IS NULL;")
        total_error_count += count
        utils.print_log("test: adressen zonder woonplaats: " + str(count), count > 0)

        count = self.fetchone("SELECT COUNT(*) FROM adressen WHERE gemeente_id IS NULL;")
        total_error_count += count
        utils.print_log("test: adressen zonder gemeente: " + str(count), count > 0)

        # Het is makkelijk om per ongeluk een gemeenten.csv te genereren die niet in UTF-8 is. Testen dus.
        naam = self.fetchone("SELECT naam FROM gemeenten WHERE id=1900")
        is_error = naam != 'Súdwest-Fryslân'
        if is_error: total_error_count += 1
        utils.print_log("test: gemeentenamen moeten in UTF-8 zijn: " + naam, is_error)

        count = self.fetchone(
            "SELECT COUNT(*) FROM adressen WHERE adressen.latitude IS NULL AND pand_id IS NOT NULL;")
        total_error_count += count
        utils.print_log("test: panden zonder locatie: " + str(count), count > 0)

        count = self.fetchone("SELECT COUNT(*) FROM adressen "
                                   "WHERE adressen.latitude IS NULL AND gebruiksdoel='ligplaats';")
        total_error_count += count
        utils.print_log("test: ligplaatsen zonder locatie: " + str(count), count > 0)

        count = self.fetchone("SELECT COUNT(*) FROM adressen "
                                   "WHERE adressen.latitude IS NULL AND gebruiksdoel='standplaats';")
        total_error_count += count
        utils.print_log("test: standplaatsen zonder locatie: " + str(count), count > 0)

        # Sommige nummers hebben een andere woonplaats dan de openbare ruimte waar ze aan liggen.
        woonplaats_id = self.fetchone(
            "SELECT woonplaats_id FROM adressen WHERE postcode='1181BN' AND huisnummer=1;")
        is_error = woonplaats_id != 1050
        if is_error: total_error_count += 1
        utils.print_log("test: nummeraanduiding WoonplaatsRef tag. 1181BN-1 ligt in Amstelveen (1050). "
                        f"Niet Amsterdam (3594): {woonplaats_id:n}", is_error)

        count = self.fetchone("SELECT COUNT(*) FROM adressen;")
        is_error = count < 9000000
        if is_error: total_error_count += 1
        utils.print_log(f"info: adressen: {count:n}", is_error)

        count = self.fetchone("SELECT COUNT(*) FROM adressen WHERE pand_id IS NOT NULL;")
        is_error = count < 9000000
        if is_error: total_error_count += 1
        utils.print_log(f"info: panden: {count:n}", is_error)

        count = self.fetchone("SELECT COUNT(*) FROM adressen WHERE object_type='ligplaats';")
        is_error = count < 10000
        if is_error: total_error_count += 1
        utils.print_log(f"info: ligplaatsen: {count:n}", is_error)

        count = self.fetchone("SELECT COUNT(*) FROM adressen WHERE object_type='standplaats';")
        is_error = count < 20000
        if is_error: total_error_count += 1
        utils.print_log(f"info: standplaatsen: {count:n}", is_error)

        count = self.fetchone("SELECT COUNT(*) FROM openbare_ruimten;")
        is_error = count < 250000
        if is_error: total_error_count += 1
        utils.print_log(f"info: openbare ruimten: {count:n}", count < is_error)

        count = self.fetchone("SELECT COUNT(*) FROM woonplaatsen;")
        is_error = count < 2000
        if is_error: total_error_count += 1
        utils.print_log(f"info: woonplaatsen: {count:n}", is_error)

        count = self.fetchone("SELECT COUNT(*) FROM gemeenten;")
        is_error = count < 300
        if is_error: total_error_count += 1
        utils.print_log(f"info: gemeenten: {count}", is_error)

        count = self.fetchone("SELECT COUNT(*) FROM provincies;")
        is_error = count != 12
        if is_error: total_error_count += 1
        utils.print_log(f"info: provincies: {count}", is_error)

        utils.print_log(f"test: total errors: {total_error_count}", total_error_count > 0)

        return total_error_count == 0

import sqlite3

import utils
import config


class DatabaseSqlite:
    connection = None
    cursor = None

    def __init__(self):
        check_same_thread = sqlite3.threadsafety != 3
        self.connection = sqlite3.connect(config.file_db_sqlite, check_same_thread=check_same_thread)
        self.cursor = self.connection.cursor()

    def close(self):
        self.connection.commit()
        self.connection.close()

    def commit(self):
        self.connection.commit()

    def fetchmany_init(self, sql):
        self.cursor.execute(sql)

    def fetchone(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchone()[0]

    def fetchall(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def fetchmany(self, size=1000):
        return self.cursor.fetchmany(size)

    def start_transaction(self):
        self.connection.execute("BEGIN TRANSACTION")

    def commit_transaction(self):
        self.connection.execute("COMMIT TRANSACTION")

    def vacuum(self):
        self.connection.execute("VACUUM")

    def save_woonplaats(self, data):
        self.connection.execute(
            """INSERT INTO woonplaatsen (woonplaats_id, naam, geometry, status, begindatum_geldigheid, einddatum_geldigheid) 
            VALUES(?, ?, ?, ?, ?, ?)""",
            (data["id"], data["naam"], data["geometry"], data["status"], data["begindatum_geldigheid"],
             data["einddatum_geldigheid"]))

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

    def save_gemeente_woonplaats(self, data):
        self.connection.execute(
            """INSERT INTO gemeente_woonplaatsen (gemeente_id, woonplaats_id, status,
            begindatum_geldigheid, einddatum_geldigheid) 
            VALUES (?, ?, ?, ?, ?);""",
            (data["gemeente_id"], data["woonplaats_id"], data["status"], data["begindatum_geldigheid"],
             data["einddatum_geldigheid"]))

    def add_gemeenten_to_woonplaatsen(self):
        self.connection.execute(
            """UPDATE woonplaatsen SET gemeente_id=gw.gemeente_id
            FROM (SELECT gemeente_id, woonplaats_id FROM gemeente_woonplaatsen) AS gw
            WHERE gw.woonplaats_id = woonplaatsen.woonplaats_id
            """)
        self.commit()

    def save_provincies(self, provincies):
        self.connection.executemany(
            "INSERT INTO provincies (id, naam) VALUES(?, ?)",
            provincies)
        self.commit()

    def save_openbare_ruimte(self, data):
        if config.use_short_street_names:
            data["naam"] = data["verkorte_naam"] if data["verkorte_naam"] != '' else data["lange_naam"]
        else:
            data["naam"] = data["lange_naam"]
        # Note: Use replace, because BAG does not always contain unique id's
        self.connection.execute(
            """REPLACE INTO openbare_ruimten (id, naam, lange_naam, verkorte_naam, type, woonplaats_id, status,
            begindatum_geldigheid, einddatum_geldigheid)
              VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (data["id"], data["naam"], data["lange_naam"], data["verkorte_naam"], data["type"], data["woonplaats_id"],
             data["status"], data["begindatum_geldigheid"], data["einddatum_geldigheid"]))

    def save_nummer(self, data):
        # Note: Use replace, because BAG does not always contain unique id's
        self.connection.execute(
            """REPLACE INTO nummers (id, postcode, huisnummer, huisletter, toevoeging, woonplaats_id, 
              openbare_ruimte_id, status, begindatum_geldigheid, einddatum_geldigheid) 
              VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (data["id"], data["postcode"], data["huisnummer"], data["huisletter"], data["toevoeging"],
             data["woonplaats_id"], data["openbare_ruimte_id"], data["status"], data["begindatum_geldigheid"],
             data["einddatum_geldigheid"])
        )

    def save_pand(self, data):
        # Note: Use replace, because BAG does not always contain unique id's
        self.connection.execute(
            """REPLACE INTO panden (id, bouwjaar, geometry, status, begindatum_geldigheid, einddatum_geldigheid)
               VALUES(?, ?, ?, ?, ?, ?)
            """,
            (data["id"], data["bouwjaar"], data["geometry"], data["status"], data["begindatum_geldigheid"],
             data["einddatum_geldigheid"])
        )

    def save_verblijfsobject(self, data):
        # Note: Use replace, because BAG does not always contain unique id's
        self.connection.execute(
            """REPLACE INTO verblijfsobjecten (id, nummer_id, pand_id, oppervlakte, rd_x, rd_y, latitude, longitude, 
            gebruiksdoel, nevenadressen, status, begindatum_geldigheid, einddatum_geldigheid) 
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (data["id"], data["nummer_id"], data["pand_id"], data["oppervlakte"], data["rd_x"], data["rd_y"],
             data["latitude"], data["longitude"], data["gebruiksdoel"], data["nevenadressen"], data["status"],
             data["begindatum_geldigheid"], data["einddatum_geldigheid"])
        )

    def save_ligplaats(self, data):
        # Note: Use replace, because BAG does not always contain unique id's
        self.connection.execute(
            """REPLACE INTO ligplaatsen (id, nummer_id, rd_x, rd_y, latitude, longitude, geometry, status,
                begindatum_geldigheid, einddatum_geldigheid)
               VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (data["id"], data["nummer_id"], data["rd_x"], data["rd_y"], data["latitude"], data["longitude"],
             data["geometry"], data["status"], data["begindatum_geldigheid"], data["einddatum_geldigheid"])
        )

    def save_standplaats(self, data):
        # Note: Use replace, because BAG does not always contain unique id's
        self.connection.execute(
            """REPLACE INTO standplaatsen (id, nummer_id, rd_x, rd_y, latitude, longitude, geometry, status,
                begindatum_geldigheid, einddatum_geldigheid)
               VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (data["id"], data["nummer_id"], data["rd_x"], data["rd_y"], data["latitude"], data["longitude"],
             data["geometry"], data["status"], data["begindatum_geldigheid"], data["einddatum_geldigheid"])
        )

    def create_bag_tables(self):
        self.connection.executescript("""
            DROP TABLE IF EXISTS gemeenten;
            CREATE TABLE gemeenten (id INTEGER PRIMARY KEY, naam TEXT, provincie_id INTEGER);
            
            DROP TABLE IF EXISTS provincies;
            CREATE TABLE provincies (id INTEGER PRIMARY KEY, naam TEXT);
            
            DROP TABLE IF EXISTS woonplaatsen;
            CREATE TABLE woonplaatsen (id INTEGER PRIMARY KEY AUTOINCREMENT, woonplaats_id INTEGER, naam TEXT, gemeente_id INTEGER, geometry TEXT,
                status TEXT, begindatum_geldigheid TEXT, einddatum_geldigheid TEXT);
            
            DROP TABLE IF EXISTS gemeente_woonplaatsen;
            CREATE TABLE gemeente_woonplaatsen (
                gemeente_id INTEGER,
                woonplaats_id INTEGER,
                status TEXT, 
                begindatum_geldigheid TEXT, 
                einddatum_geldigheid TEXT
            );              

            DROP TABLE IF EXISTS openbare_ruimten;
            CREATE TABLE openbare_ruimten (id INTEGER PRIMARY KEY, naam TEXT, lange_naam TEXT, verkorte_naam TEXT, 
                type TEXT, woonplaats_id INTEGER, status TEXT, begindatum_geldigheid TEXT, einddatum_geldigheid TEXT);

            DROP TABLE IF EXISTS nummers;
            CREATE TABLE nummers (id TEXT PRIMARY KEY, 
                postcode TEXT, 
                huisnummer INTEGER, 
                huisletter TEXT,
                toevoeging TEXT, 
                woonplaats_id TEXT, 
                openbare_ruimte_id TEXT,
                status TEXT, 
                begindatum_geldigheid TEXT, 
                einddatum_geldigheid TEXT);

            DROP TABLE IF EXISTS panden;
            CREATE TABLE panden (id TEXT PRIMARY KEY, 
                bouwjaar INTEGER, 
                geometry TEXT,
                status TEXT, 
                begindatum_geldigheid TEXT, 
                einddatum_geldigheid TEXT);

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
                begindatum_geldigheid TEXT, 
                einddatum_geldigheid TEXT);           

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
                begindatum_geldigheid TEXT, 
                einddatum_geldigheid TEXT);              

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
                begindatum_geldigheid TEXT, 
                einddatum_geldigheid TEXT);              
        """)
        self.connection.commit()

    def create_indices_bag(self):
        self.connection.executescript("""
            CREATE INDEX IF NOT EXISTS idx_verblijfsobjecten_nummer_id ON verblijfsobjecten (nummer_id);
            
            CREATE INDEX IF NOT EXISTS idx_ligplaatsen_nummer_id ON ligplaatsen (nummer_id);

            CREATE INDEX IF NOT EXISTS idx_standplaatsen_nummer_id ON standplaatsen (nummer_id);
        """)
        self.connection.commit()

    def create_indices_adressen(self):
        # Speed up woonplaatsen queries
        self.connection.executescript("""
            CREATE INDEX IF NOT EXISTS idx_adressen_woonplaats_id ON adressen (woonplaats_id)
        """)
        self.connection.commit()

    def create_adressen_from_bag(self):

        utils.print_log('create adressen tabel: import adressen')
        self.connection.executescript(f"""
            DROP TABLE IF EXISTS adressen;
            
            CREATE TABLE adressen (nummer_id TEXT PRIMARY KEY, pand_id TEXT, verblijfsobject_id TEXT, 
                gemeente_id INTEGER, woonplaats_id INTEGER, openbare_ruimte_id INTEGER, object_type TEXT, 
                gebruiksdoel TEXT, postcode TEXT, huisnummer INTEGER, huisletter TEXT, toevoeging TEXT, 
                oppervlakte FLOAT, rd_x FLOAT, rd_y FLOAT, latitude FLOAT, longitude FLOAT, bouwjaar INTEGER,
                hoofd_nummer_id TEXT, 
                geometry TEXT);

            INSERT INTO adressen (nummer_id, pand_id, verblijfsobject_id, gemeente_id, woonplaats_id, 
                openbare_ruimte_id, object_type, gebruiksdoel, postcode, huisnummer, huisletter, toevoeging, 
                oppervlakte, rd_x, rd_y, longitude, latitude, bouwjaar, geometry)
            SELECT
                n.id AS nummer_id,
                v.pand_id,
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

        utils.print_log('create adressen tabel: create indices')
        self.create_indices_adressen()

        utils.print_log('create adressen tabel: update nevenadressen data')
        self.adressen_update_nevenadressen()

        self.connection.commit()

    def adressen_import_meerdere_panden(self):

        self.connection.executescript("""
            DROP TABLE IF EXISTS temp_pand_ids;
            
            CREATE TEMP TABLE temp_pand_ids (
            nummer_id TEXT,
            pand_id TEXT
        );""")

        adressen = self.fetchall("SELECT nummer_id, pand_id FROM verblijfsobjecten WHERE pand_id LIKE '%,%'")

        parameters = []
        for adres in adressen:
            pand_ids = adres[1].split(',')
            for pand_id in pand_ids:
                parameters.append([adres[0], pand_id])

        sql = "INSERT INTO temp_pand_ids (nummer_id, pand_id) VALUES (?, ?)"
        self.connection.executemany(sql, parameters)

        # Copy bouwjaar and geometry to adressen table. Only last one remains.
        # Maybe add a multi-bouwjaar and multi-geometry option later.
        self.connection.executescript("""
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

        self.connection.commit()


    def adressen_import_ligplaatsen(self):
        self.connection.executescript("""
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
        self.connection.executescript("""
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

        self.connection.executescript("""
            DROP TABLE IF EXISTS nevenadressen;
            
            CREATE TEMP TABLE nevenadressen (
            neven_nummer_id TEXT PRIMARY KEY,
            hoofd_nummer_id TEXT
        );""")

        adressen = self.fetchall("SELECT nummer_id, nevenadressen FROM verblijfsobjecten WHERE nevenadressen <> ''")
        parameters = []
        for adres in adressen:
            neven_nummer_ids = adres[1].split(',')
            for neven_id in neven_nummer_ids:
                parameters.append([adres[0], neven_id])

        sql = "INSERT INTO nevenadressen (hoofd_nummer_id, neven_nummer_id) VALUES (?, ?)"
        self.connection.executemany(sql, parameters)
        self.connection.commit()

        self.connection.executescript("""
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
        self.connection.executescript("""
            UPDATE adressen SET
              woonplaats_id = n.woonplaats_id
            FROM (SELECT id, woonplaats_id FROM nummers WHERE woonplaats_id IS NOT '') AS n
            WHERE n.id = adressen.nummer_id;
        """)
        self.commit()

    def delete_no_longer_needed_bag_tables(self):
        self.connection.executescript("""
          DROP TABLE IF EXISTS nummers; 
          DROP TABLE IF EXISTS panden; 
          DROP TABLE IF EXISTS verblijfsobjecten; 
          DROP TABLE IF EXISTS ligplaatsen; 
          DROP TABLE IF EXISTS standplaatsen; 
        """)
        self.commit()

    def adressen_fix_bag_errors(self):
        # The BAG contains some buildings with bouwjaar 9999
        aantal = self.fetchone("SELECT COUNT(*) FROM adressen WHERE bouwjaar > 2050;")
        utils.print_log("fix: test adressen met ongeldig bouwjaar > 2050: " + str(aantal))

        if aantal > 0:
            utils.print_log(f"fix: verwijder {aantal:n} ongeldige bouwjaren (> 2100)")
            self.connection.execute("UPDATE adressen SET bouwjaar=NULL WHERE bouwjaar > 2100;")

        # The BAG contains some residences with oppervlakte 999999
        aantal = self.fetchone("SELECT COUNT(*) FROM adressen WHERE oppervlakte = 999999;")
        utils.print_log("fix: test adressen met ongeldige oppervlakte = 999999: " + str(aantal))
        if aantal > 0:
            utils.print_log(f"fix: verwijder {aantal:n} ongeldige oppervlaktes (999999)")
            self.connection.execute("UPDATE adressen SET oppervlakte=NULL WHERE oppervlakte = 999999;")

        # The BAG contains some addresses without valid public space
        aantal = self.fetchone("SELECT COUNT(*) FROM adressen WHERE openbare_ruimte_id IS NULL "
                               " OR openbare_ruimte_id NOT IN (SELECT id FROM openbare_ruimten);")
        utils.print_log("fix: test adressen zonder openbare ruimte: " + str(aantal))
        # Delete them if not too many
        if (aantal > 0) and (aantal < config.delete_addresses_without_public_spaces_if_less_than):
            utils.print_log(f"fix: verwijder {aantal:n} adressen zonder openbare ruimte")
            self.connection.execute("DELETE FROM adressen WHERE openbare_ruimte_id IS NULL "
                                    "OR openbare_ruimte_id NOT IN (SELECT id FROM openbare_ruimten)")

        self.connection.commit()

    def table_exists(self, table_name):
        # Check if database contains adressen tabel
        count = self.fetchone(f"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '{table_name}';")
        return count == 1

    def test_bag_adressen(self):
        if not self.table_exists('adressen'):
            utils.print_log("SQLite database bevat geen adressen tabel. Importeer BAG eerst.", True)
            quit()

        utils.print_log(f"start: tests op BAG SQLite database: '{config.file_db_sqlite}'")

        if self.table_exists('nummers'):
            sql = "SELECT begindatum_geldigheid FROM nummers ORDER BY begindatum_geldigheid DESC LIMIT 1"
            datum = self.fetchone(sql)
            utils.print_log(f"info: laatste nummer begindatum_geldigheid: {datum}")

        if self.table_exists('panden'):
            sql = "SELECT begindatum_geldigheid FROM panden ORDER BY begindatum_geldigheid DESC LIMIT 1"
            datum = self.fetchone(sql)
            utils.print_log(f"info: laatste pand begindatum_geldigheid: {datum}")

        # Soms zitten er nog oude gemeenten die niet meer bestaan in de gemeenten.csv filee
        aantal = self.fetchone("""
            SELECT COUNT(*) FROM gemeenten
            WHERE id NOT IN (SELECT DISTINCT gemeente_id FROM adressen);
            """)
        utils.print_log("test: gemeenten zonder adressen: " + str(aantal), aantal > 0)

        if aantal > 0:
            gemeenten = self.fetchall("""
                SELECT id, naam FROM gemeenten 
                WHERE id NOT IN (SELECT DISTINCT gemeente_id FROM adressen);
                """)

            s = ''
            for gemeente in gemeenten:
                if s:
                    s += ', '
                s += str(gemeente[0]) + ' ' + gemeente[1]
            utils.print_log("test: gemeenten zonder adressen: " + s, aantal > 0)

        aantal = self.fetchone("""
            SELECT COUNT(*) FROM woonplaatsen 
            WHERE gemeente_id IS NULL OR gemeente_id NOT IN (SELECT id FROM gemeenten);
            """)
        utils.print_log("test: woonplaatsen zonder gemeente: " + str(aantal), aantal > 0)

        aantal = self.fetchone("""
            SELECT COUNT(*) FROM adressen 
            WHERE openbare_ruimte_id IS NULL
                OR openbare_ruimte_id NOT IN (SELECT id FROM openbare_ruimten)
            """)
        utils.print_log("test: adressen zonder openbare ruimte: " + str(aantal), aantal > 0)

        aantal = self.fetchone("SELECT COUNT(*) FROM adressen WHERE woonplaats_id IS NULL;")
        utils.print_log("test: adressen zonder woonplaats: " + str(aantal), aantal > 0)

        aantal = self.fetchone("SELECT COUNT(*) FROM adressen WHERE gemeente_id IS NULL;")
        utils.print_log("test: adressen zonder gemeente: " + str(aantal), aantal > 0)

        # Het is makkelijk om per ongeluk een gemeenten.csv te genereren die niet in UTF-8 is. Testen dus.
        naam = self.fetchone("SELECT naam FROM gemeenten WHERE id=1900")
        utils.print_log("test: gemeentenamen moeten in UTF-8 zijn: " + naam, naam != 'Súdwest-Fryslân')

        aantal = self.fetchone("SELECT COUNT(*) FROM adressen WHERE adressen.latitude IS NULL AND pand_id IS NOT NULL;")
        utils.print_log("test: panden zonder locatie: " + str(aantal), aantal > 0)

        aantal = self.fetchone("SELECT COUNT(*) FROM adressen "
                               "WHERE adressen.latitude IS NULL AND gebruiksdoel='ligplaats';")
        utils.print_log("test: ligplaatsen zonder locatie: " + str(aantal), aantal > 0)

        aantal = self.fetchone(
            "SELECT COUNT(*) FROM adressen WHERE adressen.latitude IS NULL AND gebruiksdoel='standplaats';")
        utils.print_log("test: standplaatsen zonder locatie: " + str(aantal), aantal > 0)

        # Sommige nummers hebben een andere woonplaats dan de openbare ruimte waar ze aan liggen.
        woonplaats_id = self.fetchone("SELECT woonplaats_id FROM adressen WHERE postcode='1181BN' AND huisnummer=1;")
        utils.print_log("test: nummeraanduiding WoonplaatsRef tag. 1181BN-1 ligt in Amstelveen (1050). "
                        f"Niet Amsterdam (3594): {woonplaats_id:n}", woonplaats_id != 1050)

        aantal = self.fetchone("SELECT COUNT(*) FROM adressen;")
        utils.print_log(f"info: adressen: {aantal:n}", aantal < 9000000)

        aantal = self.fetchone("SELECT COUNT(*) FROM adressen WHERE pand_id IS NOT NULL;")
        utils.print_log(f"info: panden: {aantal:n}", aantal < 9000000)

        aantal = self.fetchone("SELECT COUNT(*) FROM adressen WHERE object_type='ligplaats';")
        utils.print_log(f"info: ligplaatsen: {aantal:n}", aantal < 10000)

        aantal = self.fetchone("SELECT COUNT(*) FROM adressen WHERE object_type='standplaats';")
        utils.print_log(f"info: standplaatsen: {aantal:n}", aantal < 20000)

        aantal = self.fetchone("SELECT COUNT(*) FROM openbare_ruimten;")
        utils.print_log(f"info: openbare ruimten: {aantal:n}", aantal < 250000)

        aantal = self.fetchone("SELECT COUNT(*) FROM woonplaatsen;")
        utils.print_log(f"info: woonplaatsen: {aantal:n}", aantal < 2000)

        aantal = self.fetchone("SELECT COUNT(*) FROM gemeenten;")
        utils.print_log(f"info: gemeenten: {aantal}", aantal < 300)

        aantal = self.fetchone("SELECT COUNT(*) FROM provincies;")
        utils.print_log(f"info: provincies: {aantal}", aantal != 12)

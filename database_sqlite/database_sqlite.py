import sqlite3
import config
import utils


class DatabaseSqlite:
    connection = None
    cursor = None

    def __init__(self):
        self.connection = sqlite3.connect(config.file_db_sqlite)
        self.cursor = self.connection.cursor()

    def close(self):
        self.connection.commit()
        self.connection.close()

    def commit(self):
        self.connection.commit()

    def fetchone(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchone()[0]

    def start_transaction(self):
        self.connection.execute("BEGIN TRANSACTION")

    def commit_transaction(self):
        self.connection.execute("COMMIT TRANSACTION")

    def vacuum(self):
        self.connection.execute("VACUUM")

    def save_woonplaats(self, data):
        # self.text_file.write(json.dumps(woonplaats) + '\n')
        self.connection.execute(
            "INSERT INTO woonplaatsen (id, naam) VALUES(?, ?)",
            (data["id"], data["naam"]))

    def save_gemeenten(self, gemeenten):
        self.connection.executemany(
            "INSERT INTO gemeenten (id, naam, provincie_id) VALUES(?, ?, ?);",
            gemeenten)

    def save_gemeente_woonplaats(self, data):
        self.connection.execute(
            "UPDATE woonplaatsen SET gemeente_id=? WHERE id=?;",
            (data["gemeente_id"], data["woonplaats_id"]))

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
        self.connection.execute(
            "INSERT INTO openbare_ruimten (id, naam, lange_naam, verkorte_naam, type, woonplaats_id) "
            "VALUES(?, ?, ?, ?, ?, ?)",
            (data["id"], data["naam"], data["lange_naam"], data["verkorte_naam"], data["type"], data["woonplaats_id"]))

    def save_nummer(self, data):
        # Note: Use replace, because BAG does not always contain unique id's
        self.connection.execute(
            """REPLACE INTO nummers (id, postcode, huisnummer, huisletter, toevoeging, woonplaats_id, openbareruimte_id, status)
               VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (data["id"], data["postcode"], data["huisnummer"], data["huisletter"], data["toevoeging"],
             data["woonplaats_id"], data["openbareruimte_id"], data["status"])
        )

    def save_pand(self, data):
        # Note: Use replace, because BAG does not always contain unique id's
        self.connection.execute(
            """REPLACE INTO panden (id, bouwjaar, status)
               VALUES(?, ?, ?)
            """,
            (data["id"], data["bouwjaar"], data["status"]))

    def save_verblijfsobject(self, data):
        # Note: Use replace, because BAG does not always contain unique id's
        self.connection.execute(
            """REPLACE INTO verblijfsobjecten (id, nummer_id, pand_id, oppervlakte, latitude, longitude, gebruiksdoel, 
              status) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (data["id"], data["nummer_id"], data["pand_id"], data["oppervlakte"], data["latitude"], data["longitude"],
             data["gebruiksdoel"], data["status"])
        )

    def save_ligplaats(self, data):
        # Note: Use replace, because BAG does not always contain unique id's
        self.connection.execute(
            """REPLACE INTO ligplaatsen (id, nummer_id, latitude, longitude, status)
               VALUES(?, ?, ?, ?, ?)
            """,
            (data["id"], data["nummer_id"], data["latitude"], data["longitude"], data["status"])
        )

    def save_standplaats(self, data):
        # Note: Use replace, because BAG does not always contain unique id's
        self.connection.execute(
            """REPLACE INTO standplaatsen (id, nummer_id, latitude, longitude, status)
               VALUES(?, ?, ?, ?, ?)
            """,
            (data["id"], data["nummer_id"], data["latitude"], data["longitude"], data["status"])
        )

    def create_bag_tables(self):
        self.connection.executescript("""
            DROP TABLE IF EXISTS gemeenten;
            CREATE TABLE gemeenten (id INTEGER PRIMARY KEY, naam TEXT, provincie_id INTEGER);
            
            DROP TABLE IF EXISTS provincies;
            CREATE TABLE provincies (id INTEGER PRIMARY KEY, naam TEXT);
            
            DROP TABLE IF EXISTS woonplaatsen;
            CREATE TABLE woonplaatsen (id INTEGER PRIMARY KEY, naam TEXT, gemeente_id INTEGER);
            
            DROP TABLE IF EXISTS openbare_ruimten;
            CREATE TABLE openbare_ruimten (id INTEGER PRIMARY KEY, naam TEXT, lange_naam TEXT, verkorte_naam TEXT, type TEXT, 
              woonplaats_id INTEGER);

            DROP TABLE IF EXISTS nummers;
            CREATE TABLE nummers (id TEXT PRIMARY KEY, postcode TEXT, huisnummer INTEGER, huisletter TEXT,
              toevoeging TEXT, woonplaats_id TEXT, openbareruimte_id TEXT, status TEXT);

            DROP TABLE IF EXISTS panden;
            CREATE TABLE panden (id TEXT PRIMARY KEY, bouwjaar INTEGER, status TEXT);

            DROP TABLE IF EXISTS verblijfsobjecten;
            CREATE TABLE verblijfsobjecten (id TEXT PRIMARY KEY, nummer_id TEXT, pand_id TEXT, 
              oppervlakte FLOAT, latitude FLOAT, longitude FLOAT, gebruiksdoel TEXT, status TEXT);              

            DROP TABLE IF EXISTS ligplaatsen;
            CREATE TABLE ligplaatsen (id TEXT PRIMARY KEY, nummer_id TEXT, latitude FLOAT, longitude FLOAT, 
              status TEXT);              

            DROP TABLE IF EXISTS standplaatsen;
            CREATE TABLE standplaatsen (id TEXT PRIMARY KEY, nummer_id TEXT, latitude FLOAT, longitude FLOAT, 
              status TEXT);              
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
        self.connection.executescript("""
            DROP TABLE IF EXISTS adressen;
            
            CREATE TABLE adressen (nummer_id TEXT PRIMARY KEY, pand_id TEXT, verblijfsobject_id TEXT, 
                gemeente_id INTEGER, woonplaats_id INTEGER, openbare_ruimte_id INTEGER, object_type TEXT, 
                gebruiksdoel TEXT, postcode TEXT, huisnummer INTEGER, huisletter TEXT, toevoeging TEXT, 
                oppervlakte FLOAT, latitude FLOAT, longitude FLOAT, bouwjaar INTEGER);

            INSERT INTO adressen (nummer_id, pand_id, verblijfsobject_id, gemeente_id, woonplaats_id, 
                openbare_ruimte_id, object_type, gebruiksdoel, postcode, huisnummer, huisletter, toevoeging, 
                oppervlakte, longitude, latitude, bouwjaar)
            SELECT
              n.id AS nummer_id,
              p.id AS pand_id,
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
              v.longitude,
              v.latitude,
              p.bouwjaar
            FROM nummers n
            LEFT JOIN openbare_ruimten o  ON o.id        = n.openbareruimte_id
            LEFT JOIN woonplaatsen w      ON w.id        = o.woonplaats_id
            LEFT JOIN verblijfsobjecten v ON v.nummer_id = n.id
            LEFT JOIN panden p            ON v.pand_id   = p.id;
        """)

        self.adressen_import_ligplaatsen()
        self.adressen_import_standplaatsen()
        self.adressen_update_woonplaatsen_from_nummers()
        self.create_indices_adressen()
        self.connection.commit()

    def adressen_import_ligplaatsen(self):
        self.connection.executescript("""
            UPDATE adressen SET
              latitude = l.latitude,
              longitude = l.longitude,
              object_type = 'ligplaats'
            FROM (SELECT latitude, longitude, nummer_id from ligplaatsen) AS l
            WHERE l.nummer_id = adressen.nummer_id;           
        """)

    def adressen_import_standplaatsen(self):
        self.connection.executescript("""
            UPDATE adressen SET
              latitude = s.latitude,
              longitude = s.longitude,
              object_type = 'standplaats'
            FROM (SELECT latitude, longitude, nummer_id from standplaatsen) AS s
            WHERE s.nummer_id = adressen.nummer_id;
        """)

    def adressen_update_woonplaatsen_from_nummers(self):
        self.connection.executescript("""
            UPDATE adressen SET
              woonplaats_id = n.woonplaats_id
            FROM (SELECT id, woonplaats_id from nummers WHERE woonplaats_id IS NOT '') AS n
            WHERE n.id = adressen.nummer_id;
        """)

    def delete_no_longer_needed_bag_tables(self):
        self.connection.executescript("""
          DROP TABLE IF EXISTS nummers; 
          DROP TABLE IF EXISTS panden; 
          DROP TABLE IF EXISTS verblijfsobjecten; 
          DROP TABLE IF EXISTS ligplaatsen; 
          DROP TABLE IF EXISTS standplaatsen; 
        """)

    def adressen_fix_bag_errors(self):
        # The BAG contains some buildings with bouwjaar 9999
        aantal = self.fetchone("SELECT COUNT(*) FROM adressen WHERE bouwjaar > 2100;")
        utils.print_log("fix BAG: test adressen met bouwjaar > 2100: " + str(aantal))

        if aantal > 0:
            utils.print_log(f"fix BAG: verwijder {aantal:n} ongeldige bouwjaren (> 2100)")
            self.connection.execute("UPDATE adressen SET bouwjaar=null WHERE bouwjaar > 2100;")

        # The BAG contains some residences with oppervlakte 999999
        aantal = self.fetchone("SELECT COUNT(*) FROM adressen WHERE oppervlakte = 999999;")
        utils.print_log("fix BAG: test adressen met oppervlakte = 999999: " + str(aantal))
        if aantal > 0:
            utils.print_log(f"fix BAG: verwijder {aantal:n} ongeldige oppervlaktes (999999)")
            self.connection.execute("UPDATE adressen SET oppervlakte=null WHERE oppervlakte = 999999;")

        # The BAG contains some addresses without valid public space
        aantal = self.fetchone("SELECT COUNT(*) FROM adressen WHERE openbare_ruimte_id IS NULL "
                               " OR openbare_ruimte_id NOT IN (SELECT id FROM openbare_ruimten);")
        utils.print_log("fix BAG: test adressen zonder openbare ruimte: " + str(aantal))
        # Delete them if not too many
        if (aantal > 0) and (aantal < config.delete_addresses_without_public_spaces_if_less_than):
            utils.print_log(f"fix BAG: verwijder {aantal:n} adressen zonder openbare ruimte")
            self.connection.execute("DELETE FROM adressen WHERE openbare_ruimte_id IS NULL "
                                    "OR openbare_ruimte_id NOT IN (SELECT id FROM openbare_ruimten)")

        self.connection.commit()

    def test_adressen_tabel(self):
        utils.print_log(f"start: tests on BAG SQLite database: {config.file_db_sqlite}")
        aantal = self.fetchone("""
            SELECT COUNT(*) FROM woonplaatsen 
            WHERE gemeente_id IS NULL OR gemeente_id NOT IN (SELECT ID FROM gemeenten);
            """)
        utils.print_log("test: woonplaatsen zonder gemeente: " + str(aantal))

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

        aantal = self.fetchone("SELECT COUNT(*) FROM adressen WHERE adressen.latitude IS NULL AND pand_id IS NOT NULL;")
        utils.print_log("test: panden zonder locatie: " + str(aantal), aantal > 0)

        aantal = self.fetchone("SELECT COUNT(*) FROM adressen "
                               "WHERE adressen.latitude IS NULL AND gebruiksdoel='ligplaats';")
        utils.print_log("test: ligplaatsen zonder locatie: " + str(aantal), aantal > 0)

        aantal = self.fetchone(
            "SELECT COUNT(*) FROM adressen WHERE adressen.latitude IS NULL AND gebruiksdoel='standplaats';")
        utils.print_log("test: standplaatsen zonder locatie: " + str(aantal), aantal > 0)

        # Sommige nummers hebben een andere woonplaats dan de openbare ruimte waar ze aan liggen.
        woonplaats_id = self.fetchone("SELECT woonplaats_id from adressen where postcode='1181BN' and huisnummer=1;")
        utils.print_log("test: nummeraanduiding (WoonplaatsRef tag). 1181BN-1 ligt in Amstelveen (1050). " 
                        f"Niet Amsterdam (3594): {woonplaats_id:n}", woonplaats_id != 1050)

        aantal = self.fetchone("SELECT COUNT(*) FROM adressen;")
        utils.print_log(f"info: adressen: {aantal:n}")

        aantal = self.fetchone("SELECT COUNT(*) FROM adressen WHERE pand_id IS NOT null;")
        utils.print_log(f"info: panden: {aantal:n}")

        aantal = self.fetchone("SELECT COUNT(*) FROM adressen WHERE object_type='ligplaats';")
        utils.print_log(f"info: ligplaatsen: {aantal:n}")

        aantal = self.fetchone("SELECT COUNT(*) FROM adressen WHERE object_type='standplaats';")
        utils.print_log(f"info: standplaatsen: {aantal:n}")

        aantal = self.fetchone("SELECT COUNT(*) FROM openbare_ruimten;")
        utils.print_log(f"info: openbare ruimten: {aantal:n}")

        aantal = self.fetchone("SELECT COUNT(*) FROM woonplaatsen;")
        utils.print_log(f"info: woonplaatsen: {aantal:n}")

        aantal = self.fetchone("SELECT COUNT(*) FROM gemeenten;")
        utils.print_log(f"info: gemeenten: {aantal}")

        aantal = self.fetchone("SELECT COUNT(*) FROM provincies;")
        utils.print_log(f"info: provincies: {aantal}")

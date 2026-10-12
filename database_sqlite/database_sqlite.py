import sqlite3

import utils
import config


class DatabaseSqlite:
    connection = None
    cursor = None

    def __init__(self):
        self.connection = sqlite3.connect(config.file_db_sqlite)
        self.cursor = self.connection.cursor()

        # Speed up database
        # No journaling, synchronous mode off, locking mode exclusive, RAM disk for temporary tables, cache size 200MB
        # This does not do an awful lot as saving is only 10% of the processing time. Parsing XML is 90%
        self.connection.execute("PRAGMA journal_mode=OFF;")
        self.connection.execute("PRAGMA synchronous=OFF;")
        self.connection.execute("PRAGMA locking_mode=EXCLUSIVE;")
        self.connection.execute("PRAGMA temp_store=MEMORY;")
        self.connection.execute("PRAGMA cache_size=-200000;") # 200MB. Negative value means cache size in kilobytes

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
        # Note: Inactive records (active_only=False) can have several versions of the same woonplaats id.
        # Keep the version with the latest begindatum_geldigheid.
        self.connection.execute(
            """INSERT INTO woonplaatsen (id, naam, geometry, status, begindatum_geldigheid, einddatum_geldigheid) 
            VALUES(?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              naam = excluded.naam,
              geometry = excluded.geometry,
              status = excluded.status,
              begindatum_geldigheid = excluded.begindatum_geldigheid,
              einddatum_geldigheid = excluded.einddatum_geldigheid
            WHERE excluded.begindatum_geldigheid >= woonplaatsen.begindatum_geldigheid""",
            (data["id"], data["naam"], data["geometry"], data["status"], data["begindatum_geldigheid"],
             data["einddatum_geldigheid"]))

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
            """UPDATE woonplaatsen SET gemeente_id = gw.gemeente_id
            FROM (SELECT gemeente_id, woonplaats_id FROM gemeente_woonplaatsen) AS gw
            WHERE gw.woonplaats_id = woonplaatsen.id
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
        # Note: Use REPLACE, because BAG does not always contain unique id's
        self.connection.execute(
            """REPLACE INTO openbare_ruimten (id, naam, lange_naam, verkorte_naam, type, woonplaats_id, status,
            begindatum_geldigheid, einddatum_geldigheid)
              VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (data["id"], data["naam"], data["lange_naam"], data["verkorte_naam"], data["type"], data["woonplaats_id"],
             data["status"], data["begindatum_geldigheid"], data["einddatum_geldigheid"]))

    def save_nummer(self, data):
        # Note: Use REPLACE, because BAG does not always contain unique id's
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
        # Note: Use REPLACE, because BAG does not always contain unique id's
        self.connection.execute(
            """REPLACE INTO panden (id, bouwjaar, geometry, status, begindatum_geldigheid, einddatum_geldigheid)
               VALUES(?, ?, ?, ?, ?, ?)
            """,
            (data["id"], data["bouwjaar"], data["geometry"], data["status"], data["begindatum_geldigheid"],
             data["einddatum_geldigheid"])
        )

    def save_verblijfsobject(self, data):
        # Note: Use REPLACE, because BAG does not always contain unique id's
        self.connection.execute(
            """REPLACE INTO verblijfsobjecten (id, nummer_id, oppervlakte, rd_x, rd_y, latitude, longitude,
            status, begindatum_geldigheid, einddatum_geldigheid)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (data["id"], data["nummer_id"], data["oppervlakte"], data["rd_x"], data["rd_y"],
             data["latitude"], data["longitude"], data["status"],
             data["begindatum_geldigheid"], data["einddatum_geldigheid"])
        )

        # Fields that can have multiple values are saved in separate tables
        verblijfsobject_id = data["id"]

        # Inactive records (active_only=False) can have several versions of the same verblijfsobject id. REPLACE keeps
        # the last version, so also remove the values of the earlier versions. Active records have unique id's.
        if not config.active_only:
            for table in ('verblijfsobject_panden', 'verblijfsobject_gebruiksdoelen', 'verblijfsobject_nevenadressen'):
                self.connection.execute(f"DELETE FROM {table} WHERE verblijfsobject_id = ?;", (verblijfsobject_id,))

        self.connection.executemany(
            "INSERT INTO verblijfsobject_panden (verblijfsobject_id, pand_id) VALUES (?, ?);",
            [(verblijfsobject_id, pand_id) for pand_id in data["pand_ids"]])
        self.connection.executemany(
            "INSERT INTO verblijfsobject_gebruiksdoelen (verblijfsobject_id, gebruiksdoel) VALUES (?, ?);",
            [(verblijfsobject_id, gebruiksdoel) for gebruiksdoel in data["gebruiksdoelen"]])
        self.connection.executemany(
            "INSERT INTO verblijfsobject_nevenadressen (verblijfsobject_id, nummer_id) VALUES (?, ?);",
            [(verblijfsobject_id, nummer_id) for nummer_id in data["nevenadressen"]])

    def save_ligplaats(self, data):
        # Note: Use REPLACE, because BAG does not always contain unique id's
        self.connection.execute(
            """REPLACE INTO ligplaatsen (id, nummer_id, rd_x, rd_y, latitude, longitude, geometry, status,
                begindatum_geldigheid, einddatum_geldigheid)
               VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (data["id"], data["nummer_id"], data["rd_x"], data["rd_y"], data["latitude"], data["longitude"],
             data["geometry"], data["status"], data["begindatum_geldigheid"], data["einddatum_geldigheid"])
        )

    def save_standplaats(self, data):
        # Note: Use REPLACE, because BAG does not always contain unique id's
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
            DROP TABLE IF EXISTS provincies;
            CREATE TABLE provincies (
                id INTEGER PRIMARY KEY, 
                naam TEXT);

            DROP TABLE IF EXISTS gemeenten;
            CREATE TABLE gemeenten (
                id INTEGER PRIMARY KEY, 
                naam TEXT, 
                provincie_id INTEGER);

            DROP TABLE IF EXISTS woonplaatsen;
            CREATE TABLE woonplaatsen (
                id INTEGER PRIMARY KEY, 
                naam TEXT, 
                gemeente_id INTEGER, 
                geometry TEXT,
                status TEXT, 
                begindatum_geldigheid TEXT, 
                einddatum_geldigheid TEXT);
            
            DROP TABLE IF EXISTS gemeente_woonplaatsen;
            CREATE TABLE gemeente_woonplaatsen (
                gemeente_id INTEGER,
                woonplaats_id INTEGER,
                status TEXT, 
                begindatum_geldigheid TEXT, 
                einddatum_geldigheid TEXT
            );              

            DROP TABLE IF EXISTS openbare_ruimten;
            CREATE TABLE openbare_ruimten (
                id TEXT PRIMARY KEY, 
                naam TEXT, 
                lange_naam TEXT, 
                verkorte_naam TEXT, 
                type TEXT, 
                woonplaats_id INTEGER, 
                status TEXT, 
                begindatum_geldigheid TEXT, 
                einddatum_geldigheid TEXT);

            DROP TABLE IF EXISTS nummers;
            CREATE TABLE nummers (
                id TEXT PRIMARY KEY, 
                postcode TEXT, 
                huisnummer INTEGER, 
                huisletter TEXT,
                toevoeging TEXT, 
                woonplaats_id INTEGER, 
                openbare_ruimte_id TEXT,
                status TEXT, 
                begindatum_geldigheid TEXT, 
                einddatum_geldigheid TEXT);

            DROP TABLE IF EXISTS panden;
            CREATE TABLE panden (
                id TEXT PRIMARY KEY, 
                bouwjaar INTEGER, 
                geometry TEXT,
                status TEXT, 
                begindatum_geldigheid TEXT, 
                einddatum_geldigheid TEXT);

            DROP TABLE IF EXISTS verblijfsobjecten;
            CREATE TABLE verblijfsobjecten (
                id TEXT PRIMARY KEY, 
                nummer_id TEXT,
                oppervlakte FLOAT,
                rd_x FLOAT,
                rd_y FLOAT,
                latitude FLOAT,
                longitude FLOAT,
                status TEXT,
                begindatum_geldigheid TEXT,
                einddatum_geldigheid TEXT);

            -- A verblijfsobject can have multiple panden, gebruiksdoelen and nevenadressen
            DROP TABLE IF EXISTS verblijfsobject_panden;
            CREATE TABLE verblijfsobject_panden (
                verblijfsobject_id TEXT,
                pand_id TEXT);

            DROP TABLE IF EXISTS verblijfsobject_gebruiksdoelen;
            CREATE TABLE verblijfsobject_gebruiksdoelen (
                verblijfsobject_id TEXT,
                gebruiksdoel TEXT);

            DROP TABLE IF EXISTS verblijfsobject_nevenadressen;
            CREATE TABLE verblijfsobject_nevenadressen (
                verblijfsobject_id TEXT,
                nummer_id TEXT);

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

        if not config.active_only:
            # Needed to quickly remove the values of earlier versions in save_verblijfsobject.
            # The same indices are created (if not exists) in create_indices_bag for active_only.
            self.connection.executescript("""
                CREATE INDEX IF NOT EXISTS idx_verblijfsobject_panden_verblijfsobject_id
                  ON verblijfsobject_panden (verblijfsobject_id);

                CREATE INDEX IF NOT EXISTS idx_verblijfsobject_gebruiksdoelen_verblijfsobject_id
                  ON verblijfsobject_gebruiksdoelen (verblijfsobject_id);

                CREATE INDEX IF NOT EXISTS idx_verblijfsobject_nevenadressen_verblijfsobject_id
                  ON verblijfsobject_nevenadressen (verblijfsobject_id);
            """)

        self.connection.commit()

    def create_indices_bag(self):
        self.connection.executescript("""
            CREATE INDEX IF NOT EXISTS idx_verblijfsobjecten_nummer_id ON verblijfsobjecten (nummer_id);

            CREATE INDEX IF NOT EXISTS idx_verblijfsobject_panden_verblijfsobject_id
              ON verblijfsobject_panden (verblijfsobject_id);

            CREATE INDEX IF NOT EXISTS idx_verblijfsobject_gebruiksdoelen_verblijfsobject_id
              ON verblijfsobject_gebruiksdoelen (verblijfsobject_id);

            CREATE INDEX IF NOT EXISTS idx_verblijfsobject_nevenadressen_verblijfsobject_id
              ON verblijfsobject_nevenadressen (verblijfsobject_id);
            
            CREATE INDEX IF NOT EXISTS idx_ligplaatsen_nummer_id ON ligplaatsen (nummer_id);

            CREATE INDEX IF NOT EXISTS idx_standplaatsen_nummer_id ON standplaatsen (nummer_id);
        """)
        self.connection.commit()

    def create_indices_adressen(self):
        # Speed-up woonplaatsen queries
        self.connection.executescript("""
            CREATE INDEX IF NOT EXISTS idx_adressen_woonplaats_id ON adressen (woonplaats_id);
        """)
        self.connection.commit()

    def create_adressen_from_bag(self):

        utils.print_log('create adressen tabel: combine verblijfsobject panden en gebruiksdoelen')
        self.adressen_create_temp_tables_verblijfsobjecten()

        utils.print_log('create adressen tabel: import adressen')
        self.connection.executescript(f"""
            DROP TABLE IF EXISTS adressen;
            
            CREATE TABLE adressen (
                nummer_id TEXT PRIMARY KEY, 
                nummer_begindatum_geldigheid TEXT, 
                nummer_einddatum_geldigheid TEXT, 
                pand_ids TEXT,
                pand_begindatum_geldigheid TEXT,
                pand_einddatum_geldigheid TEXT,
                verblijfsobject_id TEXT,
                gemeente_id INTEGER,
                woonplaats_id INTEGER,
                openbare_ruimte_id TEXT,
                object_type TEXT,
                gebruiksdoelen TEXT,  
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
                pand_ids,
                pand_begindatum_geldigheid,
                pand_einddatum_geldigheid,
                verblijfsobject_id,
                gemeente_id,
                woonplaats_id,
                openbare_ruimte_id,
                object_type,
                gebruiksdoelen,  
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
                vp.pand_ids,
                p.begindatum_geldigheid,
                p.einddatum_geldigheid,
                v.id AS verblijfsobject_id,
                w.gemeente_id,
                o.woonplaats_id,
                o.id,
                'verblijfsobject',
                vg.gebruiksdoelen,
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
            LEFT JOIN woonplaatsen w      ON w.id            = o.woonplaats_id
            LEFT JOIN verblijfsobjecten v ON v.nummer_id     = n.id
            LEFT JOIN temp_vbo_panden vp  ON vp.verblijfsobject_id = v.id
            LEFT JOIN temp_vbo_gebruiksdoelen vg ON vg.verblijfsobject_id = v.id
            LEFT JOIN panden p            ON p.id            = vp.eerste_pand_id;
        """)

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

    def adressen_create_temp_tables_verblijfsobjecten(self):
        # A verblijfsobject can have multiple panden and gebruiksdoelen. The adressen table has one row per address,
        # so they are combined in comma-separated fields. Only existing (active) panden are included.
        # The bouwjaar and geometry fields of an adres only contain the data of the first pand.
        self.connection.executescript("""
            DROP TABLE IF EXISTS temp_vbo_panden;
            CREATE TEMP TABLE temp_vbo_panden AS
              SELECT
                verblijfsobject_id,
                GROUP_CONCAT(pand_id) AS pand_ids,
                pand_id               AS eerste_pand_id,
                MIN(rid)
              FROM (
                SELECT vp.rowid AS rid, vp.verblijfsobject_id, vp.pand_id
                FROM verblijfsobject_panden vp
                  INNER JOIN panden ON panden.id = vp.pand_id
                ORDER BY vp.rowid
              )
              GROUP BY verblijfsobject_id;
            CREATE INDEX temp.idx_temp_vbo_panden ON temp_vbo_panden (verblijfsobject_id);

            DROP TABLE IF EXISTS temp_vbo_gebruiksdoelen;
            CREATE TEMP TABLE temp_vbo_gebruiksdoelen AS
              SELECT verblijfsobject_id, GROUP_CONCAT(gebruiksdoel) AS gebruiksdoelen
              FROM (SELECT verblijfsobject_id, gebruiksdoel FROM verblijfsobject_gebruiksdoelen ORDER BY rowid)
              GROUP BY verblijfsobject_id;
            CREATE INDEX temp.idx_temp_vbo_gebruiksdoelen ON temp_vbo_gebruiksdoelen (verblijfsobject_id);
        """)

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

            CREATE TEMP TABLE nevenadressen AS
              SELECT
                vn.nummer_id AS neven_nummer_id,
                v.nummer_id  AS hoofd_nummer_id
              FROM verblijfsobject_nevenadressen vn
                INNER JOIN verblijfsobjecten v ON v.id = vn.verblijfsobject_id;
            CREATE INDEX temp.idx_temp_nevenadressen ON nevenadressen (neven_nummer_id);
        """)

        self.connection.executescript("""
            UPDATE adressen SET
                hoofd_nummer_id = n.hoofd_nummer_id,
                pand_ids = n.pand_ids,
                verblijfsobject_id = n.verblijfsobject_id,
                gebruiksdoelen = n.gebruiksdoelen,
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
                    adressen.pand_ids,
                    adressen.verblijfsobject_id,
                    adressen.gebruiksdoelen,
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
          DROP TABLE IF EXISTS verblijfsobject_panden;
          DROP TABLE IF EXISTS verblijfsobject_gebruiksdoelen;
          DROP TABLE IF EXISTS verblijfsobject_nevenadressen;
          DROP TABLE IF EXISTS ligplaatsen; 
          DROP TABLE IF EXISTS standplaatsen; 
        """)
        self.commit()

    def adressen_remove_dummy_values(self):
        # The BAG contains dummy values in some fields (bouwjaar, oppervlakte)
        # See: https://geoforum.nl/t/zijn-dummy-waarden-in-de-bag-toegestaan/9091/5

        # Amsterdam heeft een reeks van panden met dummy bouwjaar 1005
        # https://www.amsterdam.nl/stelselpedia/bag-index/catalogus-bag/objectklasse-pand/bouwjaar-pand/
        panden = self.fetchall(f"SELECT pand_ids, bouwjaar FROM adressen WHERE bouwjaar=1005;")
        aantal = len(panden)
        utils.print_log(f"fix: test adressen met dummy bouwjaar 1005 in Amsterdam: {aantal: n}")
        if aantal > 0:
            utils.print_log(f"fix: verwijder {aantal:n} ongeldige 1005 bouwjaren")
            self.connection.execute(f"UPDATE adressen SET bouwjaar=NULL WHERE bouwjaar=1005;")

        # The BAG contains some buildings with bouwjaar 9999
        last_valid_build_year = 2040
        panden = self.fetchall(f"SELECT pand_ids, bouwjaar FROM adressen WHERE bouwjaar > {last_valid_build_year}")
        aantal = len(panden)

        utils.print_log(f"fix: test adressen met ongeldig bouwjaar > {last_valid_build_year}: {aantal: n}")

        if aantal > 0:
            utils.print_log(f"fix: verwijder {aantal:n} ongeldige bouwjaren (> {last_valid_build_year})")
            self.connection.execute(f"UPDATE adressen SET bouwjaar=NULL WHERE bouwjaar > {last_valid_build_year}")

        # The BAG contains some residences with oppervlakte 999999
        verblijfsobject_ids = self.fetchall("SELECT verblijfsobject_id FROM adressen WHERE oppervlakte = 999999;")
        aantal = len(verblijfsobject_ids)

        utils.print_log(f"fix: test adressen met ongeldige oppervlakte = 999999: {aantal: n}")
        if aantal > 0:
            utils.print_log(f"fix: verwijder {aantal:n} ongeldige oppervlaktes (999999)")
            self.connection.execute("UPDATE adressen SET oppervlakte=NULL WHERE oppervlakte = 999999;")

        # The BAG contains some residences with oppervlakte 1 (In Amsterdam this is a valid dummy)
        # https://www.amsterdam.nl/stelselpedia/bag-index/catalogus-bag/objectklasse-vbo/gebruiksoppervlakte/
        verblijfsobject_ids = self.fetchall("SELECT verblijfsobject_id FROM adressen WHERE oppervlakte = 1;")
        aantal = len(verblijfsobject_ids)
        utils.print_log(f"fix: test adressen met ongeldige oppervlakte = 1 (dummy value in Amsterdam): {aantal: n}")
        if aantal > 0:
            utils.print_log(f"fix: verwijder {aantal:n} met ongeldige oppervlakte = 1")
            self.connection.execute("UPDATE adressen SET oppervlakte=NULL WHERE oppervlakte = 1;")

        # The BAG contains some addresses without valid public space
        address_count = self.fetchone("SELECT COUNT(*) FROM adressen WHERE openbare_ruimte_id IS NULL "
                               " OR openbare_ruimte_id NOT IN (SELECT id FROM openbare_ruimten);")
        utils.print_log("fix: test adressen zonder openbare ruimte: " + str(address_count))

        # Delete them if not too many
        if (address_count > 0) and (address_count < config.delete_addresses_without_public_spaces_if_less_than):
            utils.print_log(f"fix: verwijder {address_count:n} adressen zonder openbare ruimte")
            self.connection.execute("DELETE FROM adressen WHERE openbare_ruimte_id IS NULL "
                                    "OR openbare_ruimte_id NOT IN (SELECT id FROM openbare_ruimten)")

        self.connection.commit()

    def table_exists(self, table_name):
        # Check if the database contains adressen tabel
        count = self.fetchone(f"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '{table_name}';")
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
            utils.print_log_error("SQLite database bevat geen adressen tabel. Importeer BAG eerst.")
            quit()

        utils.print_log(f"start: tests op BAG SQLite database: '{config.file_db_sqlite}'")

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
        utils.print_log_check("test: gemeenten zonder adressen: " + str(count), count > 0)

        if count > 0:
            gemeenten = self.fetchall("""
                SELECT id, naam FROM gemeenten 
                WHERE id NOT IN (SELECT DISTINCT gemeente_id FROM adressen);
                """)

            gemeenten_formatted = ', '.join(f"{gemeente[0]} {gemeente[1]}" for gemeente in gemeenten)

            utils.print_log_check("test: gemeenten zonder adressen: " + gemeenten_formatted, count > 0)

        count = self.fetchone("""
            SELECT COUNT(*) FROM woonplaatsen 
            WHERE gemeente_id IS NULL OR gemeente_id NOT IN (SELECT id FROM gemeenten);
            """)
        total_error_count += count
        utils.print_log_check("test: woonplaatsen zonder gemeente: " + str(count), count > 0)

        count = self.fetchone("""
            SELECT COUNT(*) FROM adressen 
            WHERE openbare_ruimte_id IS NULL
                OR openbare_ruimte_id NOT IN (SELECT id FROM openbare_ruimten)
            """)
        total_error_count += count
        utils.print_log_check("test: adressen zonder openbare ruimte: " + str(count), count > 0)

        count = self.fetchone("SELECT COUNT(*) FROM adressen WHERE woonplaats_id IS NULL;")
        total_error_count += count
        utils.print_log_check("test: adressen zonder woonplaats: " + str(count), count > 0)

        count = self.fetchone("SELECT COUNT(*) FROM adressen WHERE gemeente_id IS NULL;")
        total_error_count += count
        utils.print_log_check("test: adressen zonder gemeente: " + str(count), count > 0)

        # Het is makkelijk om per ongeluk een gemeenten.csv te genereren die niet in UTF-8 is. Testen dus.
        naam = self.fetchone("SELECT naam FROM gemeenten WHERE id=1900")
        is_error = naam != 'Súdwest-Fryslân'
        if is_error: total_error_count += 1
        utils.print_log_check("test: gemeentenamen moeten in UTF-8 zijn: " + naam, is_error)

        count = self.fetchone("SELECT COUNT(*) FROM adressen WHERE adressen.latitude IS NULL AND pand_ids IS NOT NULL;")
        total_error_count += count
        utils.print_log_check("test: panden zonder locatie: " + str(count), count > 0)

        count = self.fetchone("SELECT COUNT(*) FROM adressen "
                               "WHERE adressen.latitude IS NULL AND object_type='ligplaats';")
        total_error_count += count
        utils.print_log_check("test: ligplaatsen zonder locatie: " + str(count), count > 0)

        count = self.fetchone("SELECT COUNT(*) FROM adressen "
                              "WHERE adressen.latitude IS NULL AND object_type='standplaats';")
        total_error_count += count
        utils.print_log_check("test: standplaatsen zonder locatie: " + str(count), count > 0)

        # De opgeslagen latitude en longitude moeten overeenkomen met de officiele RDNAPTRANS2018 conversie van rd_x en
        # rd_y. Dit controleert een steekproef. Een database van een oudere versie (benaderingsformules) wijkt ~0.3 m af.
        from bag import coordinates
        if coordinates.pyproj is None:
            utils.print_log_warning("test: opgeslagen coordinaten niet gecontroleerd: pyproj is niet geinstalleerd")
        else:
            rows = self.fetchall("SELECT rd_x, rd_y, latitude, longitude FROM adressen "
                                 "WHERE typeof(latitude) = 'real' AND rowid % 10000 = 0;")
            wrong_count = 0
            max_error = 0.0
            for rd_x, rd_y, latitude, longitude in rows:
                lat, lon = coordinates.rd_to_wgs84(rd_x, rd_y)
                error = coordinates.distance_meters(lat, lon, latitude, longitude)
                max_error = max(max_error, error)
                if error > coordinates.MAX_ERROR_METERS: wrong_count += 1
            if wrong_count > 0: total_error_count += 1
            utils.print_log_check(f"test: opgeslagen coordinaten wijken af van RDNAPTRANS2018: {wrong_count} van "
                                  f"{len(rows)} adressen (max {max_error:.3f} m)", wrong_count > 0)

        # Rondom de Onze Lieve Vrouwetoren (Lange Jan) in Amersfoort (52.15517, 5.38720) moeten adressen liggen
        count = self.fetchone("SELECT COUNT(*) FROM adressen "
                              "WHERE latitude BETWEEN 52.1543 AND 52.1561 AND longitude BETWEEN 5.3857 AND 5.3887;")
        is_error = count == 0
        if is_error: total_error_count += 1
        utils.print_log_check(f"test: adressen binnen 100 meter van de Lange Jan in Amersfoort: {count}", is_error)

        # Sommige nummers hebben een andere woonplaats dan de openbare ruimte waar ze aan liggen.
        woonplaats_id = self.fetchone("SELECT woonplaats_id FROM adressen WHERE postcode='1181BN' AND huisnummer=1;")
        is_error = woonplaats_id != 1050
        if is_error: total_error_count += 1
        utils.print_log_check("test: nummeraanduiding WoonplaatsRef tag. 1181BN-1 ligt in Amstelveen (1050). "
                        f"Niet Amsterdam (3594): {woonplaats_id:n}", is_error)

        count = self.fetchone("SELECT COUNT(*) FROM adressen;")
        is_error = count < 9000000
        if is_error: total_error_count += 1
        utils.print_log_check(f"info: adressen: {count:n}", is_error)

        count = self.fetchone("SELECT COUNT(*) FROM adressen WHERE pand_ids IS NOT NULL;")
        is_error = count < 9000000
        if is_error: total_error_count += 1
        utils.print_log_check(f"info: panden: {count:n}", is_error)

        count = self.fetchone("SELECT COUNT(*) FROM adressen WHERE object_type='ligplaats';")
        is_error = count < 10000
        if is_error: total_error_count += 1
        utils.print_log_check(f"info: ligplaatsen: {count:n}", is_error)

        count = self.fetchone("SELECT COUNT(*) FROM adressen WHERE object_type='standplaats';")
        is_error = count < 20000
        if is_error: total_error_count += 1
        utils.print_log_check(f"info: standplaatsen: {count:n}", is_error)

        count = self.fetchone("SELECT COUNT(*) FROM openbare_ruimten;")
        is_error = count < 250000
        if is_error: total_error_count += 1
        utils.print_log_check(f"info: openbare ruimten: {count:n}", is_error)

        count = self.fetchone("SELECT COUNT(*) FROM woonplaatsen;")
        is_error = count < 2000
        if is_error: total_error_count += 1
        utils.print_log_check(f"info: woonplaatsen: {count:n}", is_error)

        count = self.fetchone("SELECT COUNT(*) FROM gemeenten;")
        is_error = count < 300
        if is_error: total_error_count += 1
        utils.print_log_check(f"info: gemeenten: {count}", is_error)

        count = self.fetchone("SELECT COUNT(*) FROM provincies;")
        is_error = count != 12
        if is_error: total_error_count += 1
        utils.print_log_check(f"info: provincies: {count}", is_error)

        utils.print_log_check(f"test: total errors: {total_error_count}", total_error_count > 0)

        return total_error_count == 0

# BAG XML parser

import os
import time
import zipfile
from _datetime import datetime
from xml.etree import ElementTree

import utils


class BagParser:
    gui_time = None
    folder_temp_xml = "temp_xml"

    def __init__(self, database):
        self.database = database
        self.count_db = 0
        self.count_xml = 0
        self.total_xml = None
        self.tag_name = None
        self.object_tag_name = None
        self.file_bag_code = None
        self.save_to_database = None
        self.start_time = None
        self.db_fields = {}
        # sometimes the same object tag is used for multiple fields and the parent tag has to be taken into account
        self.db_tag_parent_fields = {}
        self.today_string = datetime.today().strftime("%Y-%m-%d")
        self.data_init = {}

        if not os.path.exists(self.folder_temp_xml):
            os.makedirs(self.folder_temp_xml)

    def parse(self, tag_name):
        self.tag_name = tag_name
        self.count_db = 0
        self.count_xml = 0

        if self.tag_name == 'Woonplaats':
            ns_objecten = "{www.kadaster.nl/schemas/lvbag/imbag/objecten/v20200601}"
            ns_historie = "{www.kadaster.nl/schemas/lvbag/imbag/historie/v20200601}"

            self.object_tag_name = ns_objecten + tag_name
            self.file_bag_code = "9999WPL"
            self.total_xml = 3889  # required for progress indicator. Actual numbers can be found in the console or log.
            self.save_to_database = self.__save_woonplaats
            self.db_fields = {
                ns_objecten + 'identificatie': 'id',
                ns_objecten + 'naam': 'naam',
                ns_historie + 'beginGeldigheid': 'begindatum_geldigheid',
                ns_historie + 'eindGeldigheid': 'einddatum_geldigheid',
                ns_objecten + 'status': 'status',
            }
            self.db_tag_parent_fields = {}
        elif self.tag_name == 'GemeenteWoonplaatsRelatie':
            ns_gwr_product = "{www.kadaster.nl/schemas/lvbag/gem-wpl-rel/gwr-producten-lvc/v20200601}"
            ns_bagtypes = "{www.kadaster.nl/schemas/lvbag/gem-wpl-rel/bag-types/v20200601}"

            self.object_tag_name = ns_gwr_product + tag_name
            self.file_bag_code = "GEM-WPL-RELATIE"
            self.total_xml = 5765  # required for progress indicator
            self.save_to_database = self.__save_gemeente_woonplaats

            self.db_fields = {
                ns_bagtypes + 'begindatumTijdvakGeldigheid': 'begindatum_geldigheid',
                ns_bagtypes + 'einddatumTijdvakGeldigheid': 'einddatum_geldigheid',
            }
            # 'identificatie' is used for both woonplaats_id and gemeente_id.
            # Therefore, identification is done by combining the tag with the parent tag
            self.db_tag_parent_fields = {
                ns_gwr_product + 'gerelateerdeWoonplaats' + ns_gwr_product + 'identificatie': 'woonplaats_id',
                ns_gwr_product + 'gerelateerdeGemeente' + ns_gwr_product + 'identificatie': 'gemeente_id',
            }
        elif self.tag_name == 'OpenbareRuimte':
            ns_objecten = "{www.kadaster.nl/schemas/lvbag/imbag/objecten/v20200601}"
            ns_objecten_ref = "{www.kadaster.nl/schemas/lvbag/imbag/objecten-ref/v20200601}"
            ns_historie = "{www.kadaster.nl/schemas/lvbag/imbag/historie/v20200601}"
            ns_nen5825 = "{www.kadaster.nl/schemas/lvbag/imbag/nen5825/v20200601}"

            self.object_tag_name = ns_objecten + tag_name
            self.file_bag_code = "9999OPR"
            self.total_xml = 343448  # required for progress indicator
            self.data_init = {'verkorte_naam': ''}
            self.save_to_database = self.__save_openbareruimte

            self.db_fields = {
                ns_objecten + 'identificatie': 'id',
                ns_objecten + 'naam': 'lange_naam',
                ns_nen5825 + 'verkorteNaam': 'verkorte_naam',
                ns_objecten + 'type': 'type',
                ns_objecten + 'aanduidingRecordInactief': 'inactief',
                ns_historie + 'beginGeldigheid': 'begindatum_geldigheid',
                ns_historie + 'eindGeldigheid': 'einddatum_geldigheid',
                ns_objecten + 'status': 'status',
                ns_objecten_ref + 'WoonplaatsRef': 'woonplaats_id',
            }
            self.db_tag_parent_fields = {}
        elif self.tag_name == 'Nummeraanduiding':
            ns_objecten = "{www.kadaster.nl/schemas/lvbag/imbag/objecten/v20200601}"
            ns_historie = "{www.kadaster.nl/schemas/lvbag/imbag/historie/v20200601}"
            ns_objecten_ref = "{www.kadaster.nl/schemas/lvbag/imbag/objecten-ref/v20200601}"

            self.object_tag_name = ns_objecten + tag_name
            self.file_bag_code = "9999NUM"
            self.total_xml = 12054045  # required for progress indicator
            # Initialization required as BAG leaves fields out of the data if it is empty
            self.data_init = {'huisletter': '', 'toevoeging': '', 'postcode': '', 'woonplaats_id': ''}
            self.save_to_database = self.__save_nummer

            self.db_fields = {
                ns_objecten + 'identificatie': 'id',
                ns_objecten + 'postcode': 'postcode',
                ns_objecten + 'huisnummer': 'huisnummer',
                ns_objecten + 'huisletter': 'huisletter',
                ns_objecten + 'huisnummertoevoeging': 'toevoeging',
                ns_historie + 'beginGeldigheid': 'begindatum_geldigheid',
                ns_historie + 'eindGeldigheid': 'einddatum_geldigheid',
                ns_objecten + 'status': 'status',
                ns_objecten_ref + 'OpenbareRuimteRef': 'openbareruimte_id',
                ns_objecten_ref + 'WoonplaatsRef': 'woonplaats_id',
            }
            self.db_tag_parent_fields = {}
        elif self.tag_name == 'Pand':
            ns_objecten = "{www.kadaster.nl/schemas/lvbag/imbag/objecten/v20200601}"
            ns_historie = "{www.kadaster.nl/schemas/lvbag/imbag/historie/v20200601}"

            self.object_tag_name = ns_objecten + tag_name
            self.file_bag_code = "9999PND"
            self.total_xml = 20352252  # required for progress indicator
            self.data_init = {}
            self.save_to_database = self.__save_pand

            self.db_fields = {
                ns_objecten + 'identificatie': 'id',
                ns_objecten + 'oorspronkelijkBouwjaar': 'bouwjaar',
                ns_historie + 'beginGeldigheid': 'begindatum_geldigheid',
                ns_historie + 'eindGeldigheid': 'einddatum_geldigheid',
                ns_objecten + 'status': 'status',
            }
            self.db_tag_parent_fields = {}
        elif self.tag_name == 'Verblijfsobject':
            ns_objecten = "{www.kadaster.nl/schemas/lvbag/imbag/objecten/v20200601}"
            ns_historie = "{www.kadaster.nl/schemas/lvbag/imbag/historie/v20200601}"
            ns_objecten_ref = "{www.kadaster.nl/schemas/lvbag/imbag/objecten-ref/v20200601}"
            ns_gml = "{http://www.opengis.net/gml/3.2}"

            self.object_tag_name = ns_objecten + tag_name
            self.file_bag_code = "9999VBO"
            self.total_xml = 21148447  # required for progress indicator
            self.data_init = {'pos': '', 'latitude': '', 'longitude': ''}
            self.save_to_database = self.__save_verblijfsobject

            self.db_fields = {
                ns_objecten + 'identificatie': 'id',
                ns_objecten + 'oppervlakte': 'oppervlakte',
                ns_objecten + 'gebruiksdoel': 'gebruiksdoel',
                ns_gml + 'pos': 'pos',
                ns_historie + 'beginGeldigheid': 'begindatum_geldigheid',
                ns_historie + 'eindGeldigheid': 'einddatum_geldigheid',
                ns_objecten + 'status': 'status',
                ns_objecten_ref + 'PandRef': 'pand_id',
            }
            # 'nummer_id' is used for both hoofdadres and nevenadres gebruikt
            # Therefore, identification is done by combining the tag with the parent tag
            self.db_tag_parent_fields = {
                ns_objecten + 'heeftAlsHoofdadres' + ns_objecten_ref + 'NummeraanduidingRef': 'nummer_id',
            }
        elif self.tag_name == 'Ligplaats':
            ns_objecten = "{www.kadaster.nl/schemas/lvbag/imbag/objecten/v20200601}"
            ns_historie = "{www.kadaster.nl/schemas/lvbag/imbag/historie/v20200601}"
            ns_objecten_ref = "{www.kadaster.nl/schemas/lvbag/imbag/objecten-ref/v20200601}"
            ns_gml = "{http://www.opengis.net/gml/3.2}"

            self.object_tag_name = ns_objecten + tag_name
            self.file_bag_code = "9999LIG"
            self.total_xml = 17653  # required for progress indicator
            self.data_init = {'pos': '', 'latitude': '', 'longitude': ''}
            self.save_to_database = self.__save_ligplaats

            self.db_fields = {
                ns_objecten + 'identificatie': 'id',
                ns_gml + 'posList': 'pos',
                ns_objecten + 'aanduidingRecordInactief': 'inactief',
                ns_historie + 'beginGeldigheid': 'begindatum_geldigheid',
                ns_historie + 'eindGeldigheid': 'einddatum_geldigheid',
                ns_objecten + 'status': 'status',
            }
            # 'nummer_id' is used for both hoofdadres and nevenadres gebruikt
            # Therefore, identification is done by combining the tag with the parent tag
            self.db_tag_parent_fields = {
                ns_objecten + 'heeftAlsHoofdadres' + ns_objecten_ref + 'NummeraanduidingRef': 'nummer_id',
            }
        elif self.tag_name == 'Standplaats':
            ns_objecten = "{www.kadaster.nl/schemas/lvbag/imbag/objecten/v20200601}"
            ns_historie = "{www.kadaster.nl/schemas/lvbag/imbag/historie/v20200601}"
            ns_objecten_ref = "{www.kadaster.nl/schemas/lvbag/imbag/objecten-ref/v20200601}"
            ns_gml = "{http://www.opengis.net/gml/3.2}"

            self.object_tag_name = ns_objecten + tag_name
            self.file_bag_code = "9999STA"
            self.total_xml = 49543  # required for progress indicator
            self.data_init = {'pos': '', 'latitude': '', 'longitude': ''}
            self.save_to_database = self.__save_standplaats

            self.db_fields = {
                ns_objecten + 'identificatie': 'id',
                ns_gml + 'posList': 'pos',
                ns_objecten + 'aanduidingRecordInactief': 'inactief',
                ns_historie + 'beginGeldigheid': 'begindatum_geldigheid',
                ns_historie + 'eindGeldigheid': 'einddatum_geldigheid',
                ns_objecten + 'status': 'status',
            }
            # 'nummer_id' is used for both hoofdadres and nevenadres gebruikt
            # Therefore, identification is done by combining the tag with the parent tag
            self.db_tag_parent_fields = {
                ns_objecten + 'heeftAlsHoofdadres' + ns_objecten_ref + 'NummeraanduidingRef': 'nummer_id',
            }
        else:
            raise Exception("Tag name not found")

        utils.print_log(f'start: parse XML {self.tag_name}')

        self.__unzip_xml()

        xml_files = utils.find_xml_files(self.folder_temp_xml, self.file_bag_code)

        self.start_time = time.perf_counter()

        for file_xml in xml_files:
            self.__parse_file(file_xml)
        self.database.commit()

        self.__update_status(True)

        utils.print_log(f'ready: parse XML {self.tag_name} | {self.elapsed_time:.2f}s '
                        f'| XML/DB: {str(self.count_xml)}/{str(self.count_db)}')

        utils.empty_folder(self.folder_temp_xml)

    def __unzip_xml(self):
        utils.empty_folder(self.folder_temp_xml)

        file_zip = utils.find_file('temp', self.file_bag_code, 'zip')

        utils.print_log('unzip ' + file_zip)
        with zipfile.ZipFile(file_zip, 'r') as file_zip:
            file_zip.extractall(self.folder_temp_xml)

    def __update_status(self, final=False):
        if (final or
                self.gui_time is None or
                time.perf_counter() - self.gui_time > 0.5):

            self.gui_time = time.perf_counter()
            self.elapsed_time = self.gui_time - self.start_time
            tags_per_second = round(self.count_xml / self.elapsed_time)
            db_per_second = round(self.count_db / self.elapsed_time)
            text = " {:.1f}s".format(self.elapsed_time) + \
                   " | XML/DB: " + str(self.count_xml) + "/" + str(self.count_db) + \
                   " | per second XML/DB: " + str(tags_per_second) + "/" + str(db_per_second)
            if self.count_xml > self.total_xml:
                self.total_xml = self.count_xml
            utils.print_progress_bar(self.count_xml, self.total_xml, text, final)

    def __parse_file(self, file_xml):
        data = self.data_init.copy()
        tag_previous = None
        tag_now = None
        for event, elem in ElementTree.iterparse(file_xml, events=("start", "end")):
            if event == 'start':
                tag_previous = tag_now
                tag_now = elem.tag
            elif event == 'end':
                # Note: elem.text is only guaranteed in 'end' event
                if elem.tag == self.object_tag_name:
                    self.count_xml += 1
                    self.save_to_database(data)
                    data = self.data_init.copy()
                    self.__update_status()
                else:
                    field_found = False

                    if self.db_tag_parent_fields and tag_previous:
                        parent_elem_tag = tag_previous + elem.tag
                        field_parent_elem = self.db_tag_parent_fields.get(parent_elem_tag)
                        if field_parent_elem:
                            data[field_parent_elem] = elem.text
                            field_found = True

                    if not field_found:
                        field = self.db_fields.get(elem.tag)
                        if field:
                            data[field] = elem.text

    def __save_woonplaats(self, data):
        if (self.__bag_einddatum_valid(data) and
                data['status'] == "Woonplaats aangewezen"):
            self.count_db += 1
            self.__update_status()
            self.database.save_woonplaats(data)

    def __save_gemeente_woonplaats(self, data):
        if self.__bag_einddatum_valid(data) and self.__bag_begindatum_valid(data):
            self.count_db += 1
            self.__update_status()
            self.database.save_gemeente_woonplaats(data)

    def __save_openbareruimte(self, data):
        if (self.__bag_einddatum_valid(data) and
                self.__bag_begindatum_valid(data) and
                data['status'] == "Naamgeving uitgegeven"):
            self.count_db += 1
            self.__update_status()
            self.database.save_openbare_ruimte(data)

    def __save_nummer(self, data):
        if (self.__bag_einddatum_valid(data) and
                self.__bag_begindatum_valid(data) and
                data['status'] == "Naamgeving uitgegeven"):
            self.count_db += 1
            self.__update_status()
            self.database.save_nummer(data)

    def __save_pand(self, data):
        if (self.__bag_einddatum_valid(data) and
                self.__bag_begindatum_valid(data)):
            self.count_db += 1
            self.__update_status()
            self.database.save_pand(data)

    def __save_verblijfsobject(self, data):
        if (self.__bag_einddatum_valid(data) and
                self.__bag_begindatum_valid(data)):
            if data['pos']:
                [data["latitude"], data["longitude"]] = utils.bag_pos_to_coordinates(data['pos'])
            self.count_db += 1
            self.__update_status()
            self.database.save_verblijfsobject(data)

    def __save_ligplaats(self, data):
        if (self.__bag_einddatum_valid(data) and
                self.__bag_begindatum_valid(data)):
            if data['pos']:
                [data["latitude"], data["longitude"]] = utils.bag_pos_to_coordinates(data['pos'])
            self.count_db += 1
            self.__update_status()
            self.database.save_ligplaats(data)

    def __save_standplaats(self, data):
        if (self.__bag_einddatum_valid(data) and
                self.__bag_begindatum_valid(data)):
            if data['pos']:
                [data["latitude"], data["longitude"]] = utils.bag_pos_to_coordinates(data['pos'])
            self.count_db += 1
            self.__update_status()
            self.database.save_standplaats(data)

    def __bag_begindatum_valid(self, data):
        datum = data.get('begindatum_geldigheid')
        if datum:
            datum = datum[0:10]
            # string date compare is quicker than converting to date types
            return datum <= self.today_string
        else:
            return False

    def __bag_einddatum_valid(self, data):
        datum = data.get('einddatum_geldigheid')
        if datum:
            datum = datum[0:10]
            # string date compare is quicker than converting to date types
            return datum >= self.today_string
        else:
            # No einddatum means valid
            return True

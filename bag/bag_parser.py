# BAG XML parser
import math
import os
import time
from concurrent.futures import ProcessPoolExecutor, wait
from lxml import etree
from lxml.etree import ElementTree
import config

import utils
from bag import rijksdriehoek


# def parse_xml_files(files_xml, data_init, object_tag_name, db_fields, db_tag_parent_fields):
#     for file in files_xml:
#         parse_xml_file(file, data_init, object_tag_name, db_fields, db_tag_parent_fields)

def prettyprint(element, prepend=''):
    # xml = etree.tostring(element, pretty_print=True)
    # print(xml.decode(), end='')
    name = element.tag.split("}")[1][0:]
    value = f"({element.text})" if element.text and len(element.text.strip()) > 1 else ""
    attrs = ""
    for k, v in element.items():
        attrs += f"{k}={v} "
    print(f"{prepend}{name} {value} {attrs}")
    if "Polygon" != name:
        for child in element:
            prettyprint(child, prepend + '\t')


def parse_xml_file(file_xml, tag_name, data_init, object_tag_name, db_fields, db_nested_fields):
    today_string = utils.bag_date_today()

    def find_nested(bag_element, nested_list):
        # print(f"find_nested {bag_element}, {nested_list}")
        if nested_list:
            nested_list1 = nested_list.copy()
            nested_field = nested_list1.pop(0)
            field = bag_element.findall('.//{*}' + nested_field)
            if field:
                # print(f"\tfound: {field}")
                if nested_list1:
                    return find_nested(field[0], nested_list1)
                else:
                    # print(f"\tvalue: {field[0].text}")
                    return field[0].text

        # This return should not be reached
        return None

    def bag_begindatum_valid(data):
        datum = data.get('begindatum_geldigheid')
        if datum:
            datum = datum[0:10]
            # string date compare is quicker than converting to date types
            return datum <= today_string
        else:
            return False

    def bag_einddatum_valid(data):
        datum = data.get('einddatum_geldigheid')
        if datum:
            datum = datum[0:10]
            # string date compare is quicker than converting to date types
            return datum >= today_string
        else:
            return True
            # No einddatum means valid

    def data_active(data):
        status_ok = (not status_active) or (data['status'] == status_active)
        return status_ok and bag_begindatum_valid(data) and bag_einddatum_valid(data)

    # db_sqlite = DatabaseDuckdb()
    data = data_init.copy()
    coordinates_field = None
    has_geometry = False
    # 2 or 3 coordinates for geometry?
    # (Panden use 3, ligplaats & standplaats use 2)
    geometry_points = 2
    status_active = None
    # Cache data in memory to perform saving to SQLite in a single loop. SQLite is not good at threading, so
    # it needs to perform as quickly as possible
    db_data = []
    parent_tags = []
    xml_count = 0

    match tag_name:
        case 'Woonplaats':
            status_active = 'Woonplaats aangewezen'
            has_geometry = True
        case 'GemeenteWoonplaatsRelatie':
            None
        case 'OpenbareRuimte':
            status_active = 'Naamgeving uitgegeven'
        case 'Nummeraanduiding':
            status_active = 'Naamgeving uitgegeven'
        case 'Pand':
            has_geometry = True
            geometry_points = 3
            None
        case 'Verblijfsobject':
            coordinates_field = 'pos'
        case 'Ligplaats':
            coordinates_field = 'geometry'
            has_geometry = True
        case 'Standplaats':
            coordinates_field = 'geometry'
            has_geometry = True
        case _:
            raise Exception(f'No save function found for tag_name "{tag_name}"')

    # Parse XML to data
    # for event, elem in ElementTree.iterparse(file_xml, events=("start", "end")):
    #     if event == 'start':
    #         parent_tags.append(elem.tag)
    #     elif event == 'end':
    #         parent_tags.pop()
    #
    #         # Note: elem.text is only guaranteed in 'end' event
    #         if elem.tag == object_tag_name:
    #             xml_count += 1
    #             db_data.append(data)
    #             data = data_init.copy()
    #         else:
    #             field_found = False
    #
    #             if db_tag_parent_fields and parent_tags:
    #                 parent_elem_tag = parent_tags[-1] + elem.tag
    #                 field_parent_elem = db_tag_parent_fields.get(parent_elem_tag)
    #                 if field_parent_elem:
    #                     if field_parent_elem in data and data[field_parent_elem]:
    #                         data[field_parent_elem] += "," + elem.text
    #                     else:
    #                         data[field_parent_elem] = elem.text
    #                     field_found = True
    #
    #             if not field_found:
    #                 field = db_fields.get(elem.tag)
    #                 if field:
    #                     if field == 'geometry':
    #                         elem.text = '[' + elem.text + ']'
    #                     if field in data and data[field]:
    #                         data[field] += "," + elem.text
    #                     else:
    #                         data[field] = elem.text


    ### def parse_xml_file(file_xml, tag_name, data_init, object_tag_name, db_fields, db_tag_parent_fields):
    ### today_string = utils.bag_date_today()
    root = etree.parse(file_xml).getroot()
    bag_objects = root.findall(".//{*}"+object_tag_name)
    xml_count += len(bag_objects)

    for bag_object in bag_objects:
        # print(bag_object)
        data = data_init.copy()
        for xml_field, db_field  in db_fields.items():
            # print(xml_field, db_field)
            field = bag_object.findall(".//{*}" + xml_field)
            if field:
                # if 'pos' == xml_field:
                #     xml = etree.tostring(field[0], pretty_print=True)
                #     print(f"{db_field} = {xml.decode()}")
                data[db_field] = field[0].text
            else:
                data[db_field] = None

        # print(data)
        for db_field, nested_field in db_nested_fields.items():
            data[db_field] = find_nested(bag_object, nested_field)

        db_data.append(data)

    if config.active_only:
        db_data = list(filter(lambda d: data_active(d), db_data))

    if coordinates_field is not None:
        db_data = add_coordinates(db_data, coordinates_field)

    if has_geometry:
        if config.parse_geometries:
            db_data = geometry_to_wgs84(db_data, geometry_points)
        else:
            db_data = geometry_to_empty(db_data)

    # Save data to SQLite
    # for db_row in db_data:
    #     save_function(db_row)
    #
    # db_sqlite.commit()

    return {'count':xml_count, 'data':db_data}


def geometry_to_wgs84(rows, geometry_points=2):
    for i, row in enumerate(rows):
        row['geometry'] = utils.bag_geometry_to_wgs_geojson(row['geometry'], geometry_points)
    return rows


def geometry_to_empty(rows):
    for i, row in enumerate(rows):
        row['geometry'] = ''

    return rows


def get_pos_from_geometry(data):
    # geometry data looks like this: [111.111 222.222 333.333 444.444]
    # Return string with first two numbers: 111.111 222.222
    first_space = data.find(' ')
    second_space = data.find(' ', first_space + 1)

    return data[1:second_space]


def add_coordinates(rows, field_name):
    for i, row in enumerate(rows):
        if row[field_name]:
            if field_name == 'geometry':
                pos = get_pos_from_geometry(row[field_name])
            else:
                pos = row[field_name]

            [row["rd_x"], row["rd_y"]] = utils.bag_pos_to_rd_coordinates(pos)
            [row["latitude"], row["longitude"]] = rijksdriehoek.rijksdriehoek_to_wgs84(row["rd_x"], row["rd_y"])

    return rows


class BagParser:
    gui_time = None
    folder_temp_xml = "temp_xml"

    def __init__(self, database):
        self.database = database
        self.count_xml_tags = 0
        self.count_xml_files = 0
        self.total_xml_files = None
        self.tag_name = None
        self.object_tag_name = None
        self.file_bag_code = None
        self.start_time = None
        self.db_fields = {}
        # sometimes the same object tag is used for multiple fields and the parent tag has to be taken into account
        self.db_tag_parent_fields = {}
        self.today_string = utils.bag_date_today()
        self.data_init = {'status': '', 'begindatum_geldigheid': '', 'einddatum_geldigheid': ''}

        if not os.path.exists(self.folder_temp_xml):
            os.makedirs(self.folder_temp_xml)

    def parse(self, tag_name):
        self.tag_name = tag_name
        self.count_xml_tags = 0
        self.db_nested_fields = {}

        if self.tag_name == 'Woonplaats':
            ns_objecten = "{www.kadaster.nl/schemas/lvbag/imbag/objecten/v20200601}"
            ns_historie = "{www.kadaster.nl/schemas/lvbag/imbag/historie/v20200601}"
            ns_gml = "{http://www.opengis.net/gml/3.2}"

            # self.object_tag_name = ns_objecten + tag_name
            # self.file_bag_code = "9999WPL"
            # self.data_init['geometry'] = ''
            # self.db_fields = {
            #     ns_objecten + 'identificatie': 'id',
            #     ns_objecten + 'naam': 'naam',
            #     ns_historie + 'beginGeldigheid': 'begindatum_geldigheid',
            #     ns_historie + 'eindGeldigheid': 'einddatum_geldigheid',
            #     ns_objecten + 'status': 'status',
            # }
            # if config.parse_geometries:
            #     self.db_fields[ns_gml + 'posList'] = 'geometry'
            # self.db_tag_parent_fields = {}

            self.object_tag_name = tag_name
            self.file_bag_code = "9999WPL"
            self.data_init['geometry'] = ''
            self.db_fields = {
                'identificatie': 'id',
                'naam': 'naam',
                'beginGeldigheid': 'begindatum_geldigheid',
                'eindGeldigheid': 'einddatum_geldigheid',
                'status': 'status',
            }
            if config.parse_geometries:
                self.db_fields['posList'] = 'geometry'
            self.db_tag_parent_fields = {}
        elif self.tag_name == 'GemeenteWoonplaatsRelatie':
            # ns_gwr_product = "{www.kadaster.nl/schemas/lvbag/gem-wpl-rel/gwr-producten-lvc/v20200601}"
            # ns_bagtypes = "{www.kadaster.nl/schemas/lvbag/gem-wpl-rel/bag-types/v20200601}"
            #
            # self.object_tag_name = ns_gwr_product + tag_name
            # self.file_bag_code = "GEM-WPL-RELATIE"
            #
            # self.db_fields = {
            #     ns_bagtypes + 'begindatumTijdvakGeldigheid': 'begindatum_geldigheid',
            #     ns_bagtypes + 'einddatumTijdvakGeldigheid': 'einddatum_geldigheid',
            #     ns_gwr_product + 'status': 'status',
            # }
            # # 'identificatie' is used for both woonplaats_id and gemeente_id.
            # # Therefore, identification is done by combining the tag with the parent tag
            # self.db_tag_parent_fields = {
            #     ns_gwr_product + 'gerelateerdeWoonplaats' + ns_gwr_product + 'identificatie': 'woonplaats_id',
            #     ns_gwr_product + 'gerelateerdeGemeente' + ns_gwr_product + 'identificatie': 'gemeente_id',
            # }
            ns_gwr_product = "{www.kadaster.nl/schemas/lvbag/gem-wpl-rel/gwr-producten-lvc/v20200601}"
            ns_bagtypes = "{www.kadaster.nl/schemas/lvbag/gem-wpl-rel/bag-types/v20200601}"

            self.object_tag_name = tag_name
            self.file_bag_code = "GEM-WPL-RELATIE"
            self.data_init['begindatum_geldigheid'] = None
            self.data_init['einddatum_geldigheid'] = None

            self.db_fields = {
                'begindatumTijdvakGeldigheid': 'begindatum_geldigheid',
                'einddatumTijdvakGeldigheid': 'einddatum_geldigheid',
                'status': 'status',
            }
            # 'identificatie' is used for both woonplaats_id and gemeente_id.
            # Therefore, identification is done by combining the tag with the parent tag
            self.db_nested_fields = {
                # 'gerelateerdeWoonplaats' + '{*}' + 'identificatie': 'woonplaats_id',
                # 'gerelateerdeGemeente' + '{*}' + 'identificatie': 'gemeente_id',
                'woonplaats_id': ['gerelateerdeWoonplaats', 'identificatie'],
                'gemeente_id': ['gerelateerdeGemeente', 'identificatie'],
            }
        elif self.tag_name == 'OpenbareRuimte':
            # ns_objecten = "{www.kadaster.nl/schemas/lvbag/imbag/objecten/v20200601}"
            # ns_objecten_ref = "{www.kadaster.nl/schemas/lvbag/imbag/objecten-ref/v20200601}"
            # ns_historie = "{www.kadaster.nl/schemas/lvbag/imbag/historie/v20200601}"
            # ns_nen5825 = "{www.kadaster.nl/schemas/lvbag/imbag/nen5825/v20200601}"
            #
            # self.object_tag_name = ns_objecten + tag_name
            # self.file_bag_code = "9999OPR"
            # self.data_init['verkorte_naam'] = ''
            #
            # self.db_fields = {
            #     ns_objecten + 'identificatie': 'id',
            #     ns_objecten + 'naam': 'lange_naam',
            #     ns_nen5825 + 'verkorteNaam': 'verkorte_naam',
            #     ns_objecten + 'type': 'type',
            #     ns_objecten + 'aanduidingRecordInactief': 'inactief',
            #     ns_historie + 'beginGeldigheid': 'begindatum_geldigheid',
            #     ns_historie + 'eindGeldigheid': 'einddatum_geldigheid',
            #     ns_objecten + 'status': 'status',
            #     ns_objecten_ref + 'WoonplaatsRef': 'woonplaats_id',
            # }
            # self.db_tag_parent_fields = {}
            self.object_tag_name = tag_name
            self.file_bag_code = "9999OPR"
            self.data_init['naam'] = None
            # self.data_init['lange_naam'] = None
            self.data_init['verkorte_naam'] = None
            self.data_init['begindatum_geldigheid'] = None
            self.data_init['einddatum_geldigheid'] = None

            self.db_fields = {
                'identificatie': 'id',
                'naam': 'naam',
                # 'verkorteNaam': 'verkorte_naam',
                'type': 'type',
                'aanduidingRecordInactief': 'inactief',
                'beginGeldigheid': 'begindatum_geldigheid',
                'eindGeldigheid': 'einddatum_geldigheid',
                'status': 'status',
                'WoonplaatsRef': 'woonplaats_id',
            }
            self.db_nested_fields = {
                'verkorte_naam': ['verkorteNaam', 'VerkorteNaamOpenbareRuimte', 'verkorteNaam'],
            }
        elif self.tag_name == 'Nummeraanduiding':
            # ns_objecten = "{www.kadaster.nl/schemas/lvbag/imbag/objecten/v20200601}"
            # ns_historie = "{www.kadaster.nl/schemas/lvbag/imbag/historie/v20200601}"
            # ns_objecten_ref = "{www.kadaster.nl/schemas/lvbag/imbag/objecten-ref/v20200601}"
            #
            # self.object_tag_name = ns_objecten + tag_name
            # self.file_bag_code = "9999NUM"
            # # Initialization required as BAG leaves fields out of the data if it is empty
            # self.data_init['huisletter'] = ''
            # self.data_init['toevoeging'] = ''
            # self.data_init['postcode'] = ''
            # self.data_init['woonplaats_id'] = ''
            #
            # self.db_fields = {
            #     ns_objecten + 'identificatie': 'id',
            #     ns_objecten + 'postcode': 'postcode',
            #     ns_objecten + 'huisnummer': 'huisnummer',
            #     ns_objecten + 'huisletter': 'huisletter',
            #     ns_objecten + 'huisnummertoevoeging': 'toevoeging',
            #     ns_historie + 'beginGeldigheid': 'begindatum_geldigheid',
            #     ns_historie + 'eindGeldigheid': 'einddatum_geldigheid',
            #     ns_objecten + 'status': 'status',
            #     ns_objecten_ref + 'OpenbareRuimteRef': 'openbare_ruimte_id',
            #     ns_objecten_ref + 'WoonplaatsRef': 'woonplaats_id',
            # }
            # self.db_tag_parent_fields = {}
            ns_objecten = "{www.kadaster.nl/schemas/lvbag/imbag/objecten/v20200601}"
            ns_historie = "{www.kadaster.nl/schemas/lvbag/imbag/historie/v20200601}"
            ns_objecten_ref = "{www.kadaster.nl/schemas/lvbag/imbag/objecten-ref/v20200601}"

            self.object_tag_name = tag_name
            self.file_bag_code = "9999NUM"
            # Initialization required as BAG leaves fields out of the data if it is empty
            self.data_init['huisletter'] = ''
            self.data_init['toevoeging'] = ''
            self.data_init['postcode'] = ''
            self.data_init['woonplaats_id'] = None
            self.data_init['begindatum_geldigheid'] = None
            self.data_init['einddatum_geldigheid'] = None


            self.db_fields = {
                'identificatie': 'id',
                'postcode': 'postcode',
                'huisnummer': 'huisnummer',
                'huisletter': 'huisletter',
                'huisnummertoevoeging': 'toevoeging',
                'beginGeldigheid': 'begindatum_geldigheid',
                'eindGeldigheid': 'einddatum_geldigheid',
                'status': 'status',
                'OpenbareRuimteRef': 'openbare_ruimte_id',
                'WoonplaatsRef': 'woonplaats_id',
            }
            self.db_tag_parent_fields = {}

        elif self.tag_name == 'Pand':
            # ns_objecten = "{www.kadaster.nl/schemas/lvbag/imbag/objecten/v20200601}"
            # ns_historie = "{www.kadaster.nl/schemas/lvbag/imbag/historie/v20200601}"
            # ns_gml = "{http://www.opengis.net/gml/3.2}"
            #
            # self.object_tag_name = ns_objecten + tag_name
            # self.file_bag_code = "9999PND"
            # self.data_init['geometry'] = ''
            #
            # self.db_fields = {
            #     ns_objecten + 'identificatie': 'id',
            #     ns_objecten + 'oorspronkelijkBouwjaar': 'bouwjaar',
            #     ns_historie + 'beginGeldigheid': 'begindatum_geldigheid',
            #     ns_historie + 'eindGeldigheid': 'einddatum_geldigheid',
            #     ns_objecten + 'status': 'status',
            # }
            # if config.parse_geometries:
            #     self.db_fields[ns_gml + 'posList'] = 'geometry'
            # self.db_tag_parent_fields = {}
            #
            self.object_tag_name = tag_name
            self.file_bag_code = "9999PND"
            self.data_init['geometry'] = ''

            self.db_fields = {
                'identificatie': 'id',
                'oorspronkelijkBouwjaar': 'bouwjaar',
                'beginGeldigheid': 'begindatum_geldigheid',
                'eindGeldigheid': 'einddatum_geldigheid',
                'status': 'status',
            }
            if config.parse_geometries:
                self.db_fields['posList'] = 'geometry'
            # self.db_tag_parent_fields = {}

        elif self.tag_name == 'Verblijfsobject':
            # ns_objecten = "{www.kadaster.nl/schemas/lvbag/imbag/objecten/v20200601}"
            # ns_historie = "{www.kadaster.nl/schemas/lvbag/imbag/historie/v20200601}"
            # ns_objecten_ref = "{www.kadaster.nl/schemas/lvbag/imbag/objecten-ref/v20200601}"
            # ns_gml = "{http://www.opengis.net/gml/3.2}"
            #
            # self.object_tag_name = ns_objecten + tag_name
            # self.file_bag_code = "9999VBO"
            # self.data_init['pos'] = ''
            # self.data_init['rd_x'] = ''
            # self.data_init['rd_y'] = ''
            # self.data_init['latitude'] = ''
            # self.data_init['longitude'] = ''
            # self.data_init['nevenadressen'] = ''
            # self.data_init['gebruiksdoel'] = ''
            #
            # self.db_fields = {
            #     ns_objecten + 'identificatie': 'id',
            #     ns_objecten + 'oppervlakte': 'oppervlakte',
            #     ns_objecten + 'gebruiksdoel': 'gebruiksdoel',
            #     ns_gml + 'pos': 'pos',
            #     ns_historie + 'beginGeldigheid': 'begindatum_geldigheid',
            #     ns_historie + 'eindGeldigheid': 'einddatum_geldigheid',
            #     ns_objecten + 'status': 'status',
            #     ns_objecten_ref + 'PandRef': 'pand_id',
            # }
            # # 'nummer_id' is used for both hoofdadres and nevenadres gebruikt
            # # Therefore, identification is done by combining the tag with the parent tag
            # self.db_tag_parent_fields = {
            #     ns_objecten + 'heeftAlsHoofdadres' + ns_objecten_ref + 'NummeraanduidingRef': 'nummer_id',
            #     ns_objecten + 'heeftAlsNevenadres' + ns_objecten_ref + 'NummeraanduidingRef': 'nevenadressen',
            # }
            self.object_tag_name = tag_name
            self.file_bag_code = "9999VBO"
            # self.data_init['pos'] = ''
            # self.data_init['rd_x'] = ''
            # self.data_init['rd_y'] = ''
            # self.data_init['latitude'] = ''
            # self.data_init['longitude'] = ''
            # self.data_init['nevenadressen'] = ''
            # self.data_init['gebruiksdoel'] = ''

            self.db_fields = {
                'identificatie': 'id',
                'oppervlakte': 'oppervlakte',
                'gebruiksdoel': 'gebruiksdoel',
                'pos': 'pos',
                'beginGeldigheid': 'begindatum_geldigheid',
                'eindGeldigheid': 'einddatum_geldigheid',
                'status': 'status',
                'PandRef': 'pand_id',
            }
            # 'nummer_id' is used for both hoofdadres and nevenadres gebruikt
            # Therefore, identification is done by combining the tag with the parent tag
            self.db_nested_fields = {
                'nummer_id': ['heeftAlsHoofdadres', 'NummeraanduidingRef'],
                'nevenadressen': ['heeftAlsNevenadres', 'NummeraanduidingRef'],
            }

        elif self.tag_name == 'Ligplaats':
            # ns_objecten = "{www.kadaster.nl/schemas/lvbag/imbag/objecten/v20200601}"
            # ns_historie = "{www.kadaster.nl/schemas/lvbag/imbag/historie/v20200601}"
            # ns_objecten_ref = "{www.kadaster.nl/schemas/lvbag/imbag/objecten-ref/v20200601}"
            # ns_gml = "{http://www.opengis.net/gml/3.2}"
            #
            # self.object_tag_name = ns_objecten + tag_name
            # self.file_bag_code = "9999LIG"
            # self.data_init['pos'] = ''
            # self.data_init['rd_x'] = ''
            # self.data_init['rd_y'] = ''
            # self.data_init['latitude'] = ''
            # self.data_init['longitude'] = ''
            # self.data_init['geometry'] = ''
            #
            # self.db_fields = {
            #     ns_objecten + 'identificatie': 'id',
            #     ns_gml + 'posList': 'geometry',
            #     ns_objecten + 'aanduidingRecordInactief': 'inactief',
            #     ns_historie + 'beginGeldigheid': 'begindatum_geldigheid',
            #     ns_historie + 'eindGeldigheid': 'einddatum_geldigheid',
            #     ns_objecten + 'status': 'status',
            # }
            #
            # # 'nummer_id' is used for both hoofdadres and nevenadres gebruikt
            # # Therefore, identification is done by combining the tag with the parent tag
            # self.db_tag_parent_fields = {
            #     ns_objecten + 'heeftAlsHoofdadres' + ns_objecten_ref + 'NummeraanduidingRef': 'nummer_id',
            # }

            self.object_tag_name = tag_name
            self.file_bag_code = "9999LIG"
            self.data_init['pos'] = ''
            self.data_init['rd_x'] = ''
            self.data_init['rd_y'] = ''
            self.data_init['latitude'] = ''
            self.data_init['longitude'] = ''
            self.data_init['geometry'] = ''

            self.db_fields = {
                'identificatie': 'id',
                'posList': 'geometry',
                'aanduidingRecordInactief': 'inactief',
                'beginGeldigheid': 'begindatum_geldigheid',
                'eindGeldigheid': 'einddatum_geldigheid',
                'status': 'status',
            }

            # 'nummer_id' is used for both hoofdadres and nevenadres gebruikt
            # Therefore, identification is done by combining the tag with the parent tag
            self.db_nested_fields = {
                'nummer_id': ['heeftAlsHoofdadres', 'NummeraanduidingRef'],
            }
        elif self.tag_name == 'Standplaats':
            # ns_objecten = "{www.kadaster.nl/schemas/lvbag/imbag/objecten/v20200601}"
            # ns_historie = "{www.kadaster.nl/schemas/lvbag/imbag/historie/v20200601}"
            # ns_objecten_ref = "{www.kadaster.nl/schemas/lvbag/imbag/objecten-ref/v20200601}"
            # ns_gml = "{http://www.opengis.net/gml/3.2}"
            #
            # self.object_tag_name = ns_objecten + tag_name
            # self.file_bag_code = "9999STA"
            # self.data_init['pos'] = ''
            # self.data_init['rd_x'] = ''
            # self.data_init['rd_y'] = ''
            # self.data_init['latitude'] = ''
            # self.data_init['longitude'] = ''
            # self.data_init['geometry'] = ''
            #
            # self.db_fields = {
            #     ns_objecten + 'identificatie': 'id',
            #     ns_gml + 'posList': 'geometry',
            #     ns_objecten + 'aanduidingRecordInactief': 'inactief',
            #     ns_historie + 'beginGeldigheid': 'begindatum_geldigheid',
            #     ns_historie + 'eindGeldigheid': 'einddatum_geldigheid',
            #     ns_objecten + 'status': 'status',
            # }
            # # 'nummer_id' is used for both hoofdadres and nevenadres gebruikt
            # # Therefore, identification is done by combining the tag with the parent tag
            # self.db_tag_parent_fields = {
            #     ns_objecten + 'heeftAlsHoofdadres' + ns_objecten_ref + 'NummeraanduidingRef': 'nummer_id',
            # }

            self.object_tag_name = tag_name
            self.file_bag_code = "9999STA"
            self.data_init['pos'] = ''
            self.data_init['rd_x'] = ''
            self.data_init['rd_y'] = ''
            self.data_init['latitude'] = ''
            self.data_init['longitude'] = ''
            self.data_init['geometry'] = ''

            self.db_fields = {
                'identificatie': 'id',
                'posList': 'geometry',
                'aanduidingRecordInactief': 'inactief',
                'beginGeldigheid': 'begindatum_geldigheid',
                'eindGeldigheid': 'einddatum_geldigheid',
                'status': 'status',
            }
            # 'nummer_id' is used for both hoofdadres and nevenadres gebruikt
            # Therefore, identification is done by combining the tag with the parent tag
            self.db_nested_fields = {
                'nummer_id': ['heeftAlsHoofdadres', 'NummeraanduidingRef'],
            }
        else:
            raise Exception("Tag name not found")

        utils.print_log(f'start: parse {self.tag_name}')

        self.__unzip_xml()

        utils.print_log('convert XML files to DuckDB')
        self.__parse_xml_files()

        self.database.commit()

        time_elapsed = utils.time_elapsed(self.start_time)
        utils.print_log(f'ready: parse XML {self.tag_name} | {time_elapsed} '
                        f'| XML nodes: {self.count_xml_tags:,d}')

        utils.empty_folder(self.folder_temp_xml)

    def __unzip_xml(self):
        utils.empty_folder(self.folder_temp_xml)

        file_zip = utils.find_file('temp', self.file_bag_code, 'zip')

        utils.print_log('unzip ' + file_zip)
        utils.unzip_files_multithreaded(file_zip, self.folder_temp_xml)

    def __parse_xml_files(self):

        post_sql = None
        match self.tag_name:
            case 'Woonplaats':
                save_function = self.database.save_woonplaats
            case 'GemeenteWoonplaatsRelatie':
                save_function = self.database.save_gemeente_woonplaats
            case 'OpenbareRuimte':
                save_function = self.database.save_openbare_ruimte
                # for data in datarows:
                #     if config.use_short_street_names:
                #         data["naam"] = data["verkorte_naam"] if data["verkorte_naam"] != '' else data["lange_naam"]
                #     else:
                #         data["naam"] = data["lange_naam"]
                if config.use_short_street_names:
                    post_sql = 'UPDATE openbare_ruimten SET naam=verkorte_naam WHERE verkorte_naam is not NULL';
            case 'Nummeraanduiding':
                save_function = self.database.save_nummer
            case 'Pand':
                save_function = self.database.save_pand
            case 'Verblijfsobject':
                save_function = self.database.save_verblijfsobject
                post_sql = "UPDATE verblijfsobjecten SET lon_lat=st_point(longitude, latitude) WHERE longitude is not NULL and latitude is not NULL"
            case 'Ligplaats':
                save_function = self.database.save_ligplaats
                post_sql = "UPDATE ligplaatsen SET lon_lat=st_point(longitude, latitude) WHERE longitude is not NULL and latitude is not NULL"
            case 'Standplaats':
                save_function = self.database.save_standplaats
                post_sql = "UPDATE standplaatsen SET lon_lat=st_point(longitude, latitude) WHERE longitude is not NULL and latitude is not NULL"
            case _:
                raise Exception(f'No save function found for tag_name "{self.tag_name}"')

        xml_files = utils.find_xml_files(self.folder_temp_xml, self.file_bag_code)
        files_total = len(xml_files)
        self.total_xml_files = files_total
        self.count_xml_files = 0

        self.start_time = time.perf_counter()

        workers_count = config.cpu_cores_used

        futures = []
        # Multi-threading and batch
        # use ceil instead of round to prevent zero batch size
        batch_size = math.ceil(files_total / workers_count)

        # Multi-threading. One XML file per executor.
        pool = ProcessPoolExecutor(workers_count)
        for file_xml in xml_files:
            future = pool.submit(parse_xml_file, file_xml, self.tag_name, self.data_init, self.object_tag_name,
                                 self.db_fields,
                                 self.db_nested_fields)
            futures.append(future)

        for future in futures:
            result = future.result()
            count_file_xml = result['count']

            save_function(result['data'])

            self.count_xml_files += 1
            self.count_xml_tags += count_file_xml
            self.__update_xml_status()

        wait(futures)
        self.__update_xml_status(True)
        if post_sql:
            utils.print_log(f"Post processing {self.tag_name}")
            self.database.post_process(post_sql)


    def add_gemeenten_into_woonplaatsen(self):
        if (not config.active_only):
            utils.print_log('gemeente_id is only added to woonplaatsen if active_only=True in config', True)
            return
        self.database.add_gemeenten_to_woonplaatsen()

    def __update_xml_status(self, final=False):
        if (final or
                (self.gui_time is None) or
                (time.perf_counter() - self.gui_time > 0.5)):

            self.gui_time = time.perf_counter()
            self.elapsed_time = self.gui_time - self.start_time
            tags_per_second = round(self.count_xml_tags / self.elapsed_time)
            time_elapsed_text = utils.time_elapsed(self.start_time)

            bar_text = f" {time_elapsed_text} | XML nodes: {self.count_xml_tags:,d} | per second: {tags_per_second:,d}"
            utils.print_progress_bar(self.count_xml_files, self.total_xml_files, bar_text, final)

import json
import math
import sqlite3
import sys
import urllib.parse
import urllib.request
from argparse import ArgumentParser

import config
import utils

PDOK_BAG_URL = 'https://api.pdok.nl/kadaster/bag/ogc/v2/collections/verblijfsobject/items'
CRS_RD = 'http://www.opengis.net/def/crs/EPSG/0/28992'
MAX_DIFFERENCE_METERS = 0.001

parser = ArgumentParser(description='Compare the RD coordinates of addresses in the SQLite database with the BAG API '
                                    'of PDOK. This checks if the parser reads the correct positions from the BAG. '
                                    'Needs internet access.')
parser.add_argument('-n', '--count', type=int, default=20,
                    help="Number of addresses to compare, evenly spread over the database (default: 20)")
parser.add_argument('-d', '--database', default=config.file_db_sqlite,
                    help=f"SQLite database to check (default: {config.file_db_sqlite})")
args = parser.parse_args()


def get_pdok_point(verblijfsobject_id):
    """Get the RD position of a verblijfsobject from the BAG API of PDOK. Returns (x, y), or None if not found."""
    params = {'f': 'json', 'identificatie': verblijfsobject_id, 'crs': CRS_RD}
    request = urllib.request.Request(PDOK_BAG_URL + '?' + urllib.parse.urlencode(params),
                                     headers={'Accept': 'application/json'})
    with urllib.request.urlopen(request, timeout=60) as response:
        features = json.load(response)['features']
    if not features:
        return None
    geometry = features[0]['geometry']
    if geometry['type'] != 'Point':
        raise Exception(f"PDOK geometry is a {geometry['type']}, not a Point")
    return geometry['coordinates'][0], geometry['coordinates'][1]


try:
    db = sqlite3.connect(f'file:{args.database}?mode=ro', uri=True)
    total = db.execute("SELECT MAX(rowid) FROM adressen").fetchone()[0]
except sqlite3.Error as e:
    utils.print_log_error(f"could not read the adressen table of '{args.database}': {e}")
    sys.exit(1)

# Addresses of verblijfsobjecten with a position, evenly spread over the table
addresses = []
seen = set()
for i in range(args.count):
    start = int(total * (i + 0.5) / args.count)
    row = db.execute("SELECT verblijfsobject_id, postcode, huisnummer, rd_x, rd_y FROM adressen "
                     "WHERE rowid >= ? AND object_type = 'verblijfsobject' AND verblijfsobject_id IS NOT NULL "
                     "AND typeof(rd_x) = 'real' LIMIT 1", (start,)).fetchone()
    if row and row[0] not in seen:
        seen.add(row[0])
        addresses.append(row)

utils.print_log(f"compare {len(addresses)} addresses of '{args.database}' with the BAG API of PDOK")

different = []
not_found = []
failed = []
for verblijfsobject_id, postcode, huisnummer, rd_x, rd_y in addresses:
    try:
        point = get_pdok_point(verblijfsobject_id)
    except Exception as e:
        failed.append(verblijfsobject_id)
        utils.print_log_error(f"{postcode} {huisnummer} (verblijfsobject {verblijfsobject_id}): {e}")
        continue

    if point is None:
        not_found.append(verblijfsobject_id)
        utils.print_log_warning(f"{postcode} {huisnummer} (verblijfsobject {verblijfsobject_id}): not found at PDOK")
        continue

    difference = math.hypot(point[0] - rd_x, point[1] - rd_y)
    text = (f"{postcode} {huisnummer} (verblijfsobject {verblijfsobject_id}): difference {difference:.4f} m "
            f"(ours {rd_x:.3f}, {rd_y:.3f} | PDOK {point[0]:.3f}, {point[1]:.3f})")
    if difference > MAX_DIFFERENCE_METERS:
        different.append(verblijfsobject_id)
        utils.print_log_error(text)
    else:
        utils.print_log(text)

compared = len(addresses) - len(not_found) - len(failed)
utils.print_log(f"compared: {compared} | different from PDOK: {len(different)} | not found: {len(not_found)} | "
                f"errors: {len(failed)}")
if different or failed:
    utils.print_log("Differences can also be caused by changes in the BAG since the extract that you parsed.")
sys.exit(1 if (different or failed) else 0)

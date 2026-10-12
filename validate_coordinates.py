import json
import math
import re
import statistics
import sys
import urllib.request
from argparse import ArgumentParser

import utils
from bag import coordinates

RDINFO_URL = 'https://api.pdok.nl/kadaster/rdinfo/ogc/v1/collections/stations/items?f=json&limit=1000'

# Places to take the reference points from. The nearest GNSS station of each place is used.
REFERENCE_PLACES = [
    ('Den Helder', 110000, 550000), ('Groningen', 233000, 582000), ('Enschede', 258000, 470000),
    ('Maastricht', 176000, 318000), ('Vlissingen', 30000, 385000), ('Amersfoort', 155000, 463000),
    ('Amsterdam', 121000, 487000), ('Rotterdam', 92000, 437000), ('Eindhoven', 161000, 383000),
    ('Zwolle', 205000, 504000),
]

parser = ArgumentParser(description='Validate the RD to latitude and longitude (ETRS89) conversion against the official GNSS stations of the '
                                    'Kadaster (RDinfo, open data on PDOK). Needs internet access.')
parser.add_argument('-u', '--update-reference-points', action='store_true',
                    help="Update the table with reference points in bag/coordinates.py with the latest RDinfo data")
parser.add_argument('--url', default=RDINFO_URL, help="Location of the RDinfo stations (default: PDOK)")
args = parser.parse_args()


def dms_to_degrees(text):
    # RDinfo has degrees, minutes and seconds with a decimal comma. For example: 53 27 27,04098
    degrees, minutes, seconds = text.replace(',', '.').split()
    return float(degrees) + float(minutes) / 60 + float(seconds) / 3600


def download_stations(url):
    """Download all GNSS stations with RD and ETRS89 coordinates from RDinfo."""
    stations = []
    total_bytes = 0
    pages = 0
    while url and pages < 100:
        request = urllib.request.Request(url, headers={'Accept': 'application/json'})
        with urllib.request.urlopen(request, timeout=60) as response:
            body = response.read()
        total_bytes += len(body)
        pages += 1
        data = json.loads(body)
        for feature in data['features']:
            p = feature['properties']
            if p.get('phi') and p.get('lambda') and p.get('xrd') is not None and p.get('yrd') is not None:
                stations.append(dict(punt=p['punt'], station=p['station'], year=p['jaar'], x=p['xrd'], y=p['yrd'],
                                     lat=dms_to_degrees(p['phi']), lon=dms_to_degrees(p['lambda'])))
        url = next((link['href'] for link in data.get('links', []) if link.get('rel') == 'next'), None)
    utils.print_log(f"downloaded {pages} pages ({total_bytes / 1024 / 1024:.1f} MB) from RDinfo")
    return stations


def update_reference_points(stations):
    used = set()
    lines = []
    for place, x, y in REFERENCE_PLACES:
        s = min((s for s in stations if (s['punt'], s['station']) not in used),
                key=lambda s: math.hypot(s['x'] - x, s['y'] - y))
        used.add((s['punt'], s['station']))
        lines.append(f"    ({s['x']:.4f}, {s['y']:.4f}, {s['lat']:.9f}, {s['lon']:.9f}),  "
                     f"# {place}: RDinfo punt {s['punt']} station {s['station']} ({s['year']})\n")

    path = coordinates.__file__
    with open(path, encoding='utf-8', newline='') as f:
        text = f.read()
    newline = '\r\n' if '\r\n' in text else '\n'
    new_text, count = re.subn(r'REFERENCE_POINTS = \[.*?\n\]', lambda m: 'REFERENCE_POINTS = [\n' + ''.join(lines).rstrip('\n') + '\n]',
                              text.replace('\r\n', '\n'), flags=re.DOTALL)
    if count != 1:
        utils.print_log_error("could not find the REFERENCE_POINTS table in bag/coordinates.py")
        sys.exit(1)
    with open(path, 'w', encoding='utf-8', newline='') as f:
        f.write(new_text.replace('\n', newline))
    utils.print_log(f"updated the {len(lines)} reference points in {path}")


# The conversion needs pyproj and the grid file. The reference points are not checked here, because they are what is
# being validated.
if coordinates.pyproj is None:
    utils.print_log_error("The required package 'pyproj' is not installed. Install it with: pip install -r requirements.txt")
    sys.exit(1)

try:
    stations = download_stations(args.url)
except Exception as e:
    utils.print_log_error(f"could not download the stations from RDinfo: {e}")
    sys.exit(1)

if not stations:
    utils.print_log_error("RDinfo returned no GNSS stations with RD and ETRS89 coordinates")
    sys.exit(1)

errors = []
for s in stations:
    lat, lon = coordinates.rd_to_etrs89(s['x'], s['y'])
    errors.append((coordinates.distance_meters(lat, lon, s['lat'], s['lon']), s))

values = [e for e, s in errors]
too_far = [(e, s) for e, s in errors if e > coordinates.MAX_ERROR_METERS]
utils.print_log(f"GNSS stations with official RD and ETRS89 coordinates: {len(stations)}")
utils.print_log(f"difference with our conversion: mean {statistics.mean(values) * 1000:.2f} mm | "
                f"median {statistics.median(values) * 1000:.2f} mm | max {max(values) * 1000:.2f} mm")
utils.print_log_check(f"stations further than {coordinates.MAX_ERROR_METERS * 100:.0f} cm from the official coordinates: "
                      f"{len(too_far)}", len(too_far) > 0)
for error, s in sorted(too_far, key=lambda item: item[0], reverse=True)[:5]:
    utils.print_log_error(f"punt {s['punt']} station {s['station']} ({s['year']}): {error:.3f} m")

if args.update_reference_points:
    update_reference_points(stations)

sys.exit(1 if too_far else 0)

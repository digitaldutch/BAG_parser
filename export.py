#! /usr/bin/env python3
from argparse import ArgumentParser

from exporter import Exporter

parser = ArgumentParser(description='Export addresses in DuckDB database to a CSV, Parquet or JSON file')

helpText = ("Export all data including year of construction, latitude, longitude, floor area and intended use of "
            "buildings")
parser.add_argument('-a', '--all', action='store_true', help=helpText)

helpText = "Export statistics of 4 character postal code groups (e.g. 1000)"
parser.add_argument('-p4', '--postcode4', action='store_true', help=helpText)

helpText = "Export statistics of 5 character postal code groups (e.g. 1000A)"
parser.add_argument('-p5', '--postcode5', action='store_true', help=helpText)

helpText = "Export statistics of 6 character postal code groups (e.g. 1000AA)"
parser.add_argument('-p6', '--postcode6', action='store_true', help=helpText)

helpText = "Export to parquet rather than CSV"
parser.add_argument('--parquet', action='store_true', help=helpText)

helpText = "Export to JSON rather than CSV"
parser.add_argument('--json', action='store_true', help=helpText)

args = parser.parse_args()

exporter = Exporter()

ext = 'csv'
export_options = "(HEADER, DELIMITER ',')"
if args.parquet:
    ext = 'parquet'
    export_options = "(FORMAT parquet)"
elif args.json:
    ext = 'json'
    export_options = "(ARRAY)"

if args.all:
    exporter.export(f'output/adressen_all_data.{ext}', export_options)
elif args.postcode4:
    exporter.export_postcode4_stats(f'output/adressen_p4_stats.{ext}', export_options)
elif args.postcode5:
    exporter.export_postcode5_stats(f'output/adressen_p5_stats.{ext}', export_options)
elif args.postcode6:
    exporter.export_postcode6_stats(f'output/adressen_p6_stats.{ext}', export_options)
else:
    exporter.export_postcode(f'output/adressen_postcodes.{ext}', export_options)

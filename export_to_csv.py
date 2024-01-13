from argparse import ArgumentParser

from exporter import Exporter

parser = ArgumentParser(description='Export addresses in SQLite database to a *.csv file')

helpText = ("Export all data including year of construction, latitude, longitude, floor area and intended use of "
            "buildings")
parser.add_argument('-a', '--all', action='store_true', help=helpText)

helpText = "Export statistics of 4 character postal code groups (e.g. 1000)"
parser.add_argument('-p4', '--postcode4', action='store_true', help=helpText)

helpText = "Export statistics of 5 character postal code groups (e.g. 1000A)"
parser.add_argument('-p5', '--postcode5', action='store_true', help=helpText)

helpText = "Export statistics of 6 character postal code groups (e.g. 1000AA)"
parser.add_argument('-p6', '--postcode6', action='store_true', help=helpText)

args = parser.parse_args()

csv_exporter = Exporter()

if args.all:
    csv_exporter.export_to_csv('output/adressen_all_data.csv')
elif args.postcode4:
    csv_exporter.export_to_csv_postcode4_stats('output/adressen_p4_stats.csv')
elif args.postcode5:
    csv_exporter.export_to_csv_postcode5_stats('output/adressen_p5_stats.csv')
elif args.postcode6:
    csv_exporter.export_to_csv_postcode6_stats('output/adressen_p6_stats.csv')
else:
    csv_exporter.export_to_csv_postcode('output/adressen_postcodes.csv')

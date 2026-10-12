[![GitHub license](https://img.shields.io/github/license/digitaldutch/BAG_parser)](https://github.com/digitaldutch/BAG_parser/blob/master/LICENSE)
[![GitHub issues](https://img.shields.io/github/issues/digitaldutch/BAG_parser)](https://github.com/digitaldutch/BAG_parser/issues)

# Digital Dutch BAG parser

## TL;DR
Converts in a few minutes the big, complex and hard to read XML Dutch addresses database (BAG or Basisregistratie Adressen
en Gebouwen) into a user-friendly, file-based, blazingly fast SQLite database by running a single Python script. 
No database server needed. You only need Python. The package [pyproj](https://pyproj4.github.io/pyproj/) is recommended for the most accurate coordinates.

Another oneliner script will convert this SQLite database to CSV in 15 seconds. 

## Download the parsed BAG
If you are too lazy to run the script yourself, you can also download the BAG in SQLite or CSV format from
our [releases](https://github.com/digitaldutch/BAG_parser/releases) section.

## About the BAG
The Dutch public addresses and buildings database (BAG or Basisregistratie Adressen en Gebouwen) is freely downloadable
from the [Dutch cadastre](https://www.kadaster.nl/-/gratis-download-bag-extract) agency named Kadaster. Hooray 🙂. 

The bad news is: The original BAG comes in a complex and hard to read XML format using thousands of zipped XML files, 
which will quickly reduce your initial enthusiasm. 
It also does not include municipalities or provinces and provides coordinates using a system that non-experts won't 
expect named 
 [Rijksdriehoekscoördinaten](https://nl.wikipedia.org/wiki/Rijksdriehoeksco%C3%B6rdinaten)😲. 

## What this parser does
This Python utility parses the BAG database and converts it into a clean, easy to read & use SQLite database.
Municipalities (gemeenten) and provinces (provincies) are added. Rijksdriehoekscoördinaten coordinates are converted 
to latitude and longitude coordinates: ETRS89 (the European standard). It is close (less than 1 m, see [Coordinates](#coordinates)) to WGS84 coordinates. But if you want exact WGS84 coordinates, you can select that in the [config.py](config.py). 

Invalid (dummy) bouwjaar and oppervlakte fields are removed. 
Construction year, floor area and intended use of buildings are also provided. Several tables (nummers, verblijfsobjecten, panden, ligplaatsen and standplaatsen) are merged into a general 'adressen'
table. 

The SQLite database can be used directly as a source to generate a *.csv file or to update your own addresses 
databases. 

There are several settings you can tweak in the [config.py](config.py).

## Requirements
* Python 3.11 or newer.
* The `pyproj` package. Recommended. It converts the BAG coordinates (Rijksdriehoekscoördinaten) to latitude and longitude (ETRS89 or WGS84, see [Coordinates](#coordinates)) with high accuracy. The parser also needs the grid file in the [proj](proj) folder. It is included, so you don't have to download anything.
  Without `pyproj` the parser still works, but it shows a warning and the coordinates are less accurate, see [Without pyproj](#without-pyproj).
  The `WGS84` coordinates option needs `pyproj`.
* The `psutil` package. Optional. It detects the number of physical CPU cores for the best performance.
  Without it the parser shows a warning and uses the number of logical CPU cores, which can be higher
  (see `cpu_cores_used` in [config.py](config.py)).

## Installation
You only have to do this once. This parser runs without additional packages. Steps 3 to 5 are optional install `pyproj`, 
which is not needed but provides the most accurate coordinates, see [Requirements](#requirements).
1. Install [Python](https://www.python.org/downloads/) 3.11 or newer.
2. Download the BAG parser. Using git is recommended as updates are easier:
   ```
   git clone https://github.com/digitaldutch/BAG_parser
   ```
3. Open a terminal in the BAG parser folder and create a virtual environment (recommended):
   ```
   python -m venv .venv
   ```
   (PyCharm and other editors can create this environment for you.)
4. Activate the virtual environment. On Windows:
   ```
   .venv\Scripts\activate
   ```
   On Linux and macOS:
   ```
   source .venv/bin/activate
   ```
5. Install the recommended packages:
   ```
   pip install -r requirements.txt
   ```

## Running the parser
Do this each time you want a new SQLite database, for example, when there is a new BAG release.
1. Open a terminal in the BAG parser folder and activate the virtual environment (see Installation, step 4).
   You have to do this in every new terminal. PyCharm and other editors do this automatically.
2. Optional: update the BAG parser to the latest version and install new requirements if there are any.
   The parser tells you at startup if a newer version is available. It checks this with one web request to GitHub.
   Set `check_for_updates = False` in [config.py](config.py) to turn this off.
   ```
   git pull https://github.com/digitaldutch/BAG_parser
   pip install -r requirements.txt
   ```
3. Download the BAG (3 GB) from [kadaster.nl](https://www.kadaster.nl/-/gratis-download-bag-extract)
   or directly from [pdok.nl](https://service.pdok.nl/kadaster/adressen/atom/v1_0/downloads/lvbag-extract-nl.zip)
   and save the file as `bag.zip` in the `input` folder. Replace the old file if there is one.
4. The [gemeenten.csv](input/gemeenten.csv) file is already included in the `input` folder, but you can [download the latest version from the CBS website](https://www.cbs.nl/nl-nl/onze-diensten/methoden/classificaties/overig/gemeentelijke-indelingen-per-jaar). Save it as `gemeenten.csv` in the input folder.
5. Set your options in [config.py](config.py). The defaults are fine to start with.
6. Run the importer:
   ```
   python import_bag.py
   ```
7. Drink a cup of coffee for a few minutes ☕😎 while watching the progress bar.
8. Open the SQLite database (`output/bag.sqlite`) with your favorite tool. I like [DBeaver](https://dbeaver.io/).

## Usage
Here's an example query on SQLite database to get information about postcode 2514GL, huisnummer 78 (Paleis Noordeinde):
```SQL
SELECT
  a.postcode,
  a.huisnummer,
  a.huisletter || a.toevoeging AS toevoeging,
  o.naam                       AS straat,
  g.naam                       AS gemeente,
  w.naam                       AS woonplaats,
  p.naam                       AS provincie,
  a.bouwjaar,
  a.latitude,
  a.longitude,
  a.rd_x,
  a.rd_y,
  a.oppervlakte                AS vloeroppervlakte,
  a.gebruiksdoelen,
  a.hoofd_nummer_id
FROM adressen a
  LEFT JOIN openbare_ruimten o ON a.openbare_ruimte_id = o.id
  LEFT JOIN gemeenten g        ON a.gemeente_id        = g.id
  LEFT JOIN woonplaatsen w     ON a.woonplaats_id      = w.id
  LEFT JOIN provincies p       ON g.provincie_id       = p.id
WHERE postcode = '2514GL'
  AND huisnummer = 78;
```
* When done parsing, use [export_to_csv.py](export_to_csv.py) to create a *.csv file. This file has several command line options (see below). 
These conversion functions are easy to customize. 

## Python commands ##

### [import_bag.py](import_bag.py)
Parses the original BAG file and transforms it into an SQLite database. Takes about 7 minutes to complete
on an AMD 9900X PC, or a few minutes more if you switch on the `parse_geometries` option in the [config.py](config.py).

### [export_to_csv.py](export_to_csv.py)
Exports the addresses in SQLite database to a *.csv file. By default, only the addresses and
postcode data are exported (~15 seconds). Use the command options below for more output formats.  

-a, --adressen  
Export all address data including year of construction, latitude, longitude, floor area and intended use of buildings.
~40s. The old name `--all` still works.

-h, --help  
Show the help information message

-o, --openbare-ruimten  
Export all active openbare ruimten (streets etc.), including those without addresses. Includes woonplaats, gemeente and provincie. ~1s

-w, --woonplaatsen  
Export all active woonplaatsen with gemeente and provincie. ~1s

-p4, --postcode4  
Export statistics of 4 character postal code groups. (e.g. 1000). ~10s 

-p5, --postcode5  
Export statistics of 5 character postal code groups (e.g. 1000A). ~10s

-p6, --postcode6  
Export statistics of 6 character postal code groups (e.g. 1000AA). ~10s

### [validate_coordinates.py](validate_coordinates.py)
Validates the conversion of the Rijksdriehoekscoördinaten against the official GNSS stations of the Kadaster
([RDinfo](https://api.pdok.nl/kadaster/rdinfo/ogc/v1), open data on PDOK). It downloads about 440 stations (6.7 MB), converts
their RD coordinates and compares the result with their official ETRS89 coordinates. It needs internet access.
The parser also checks 10 of these stations each time it starts. These are in `bag/coordinates.py`.
Run this script now and then, for example after updating `pyproj`, because the Kadaster can update the data.

-u, --update-reference-points  
Update the 10 reference points in `bag/coordinates.py` with the latest RDinfo data.

### [validate_coordinates_with_pdok.py](validate_coordinates_with_pdok.py)
Checks if the parser reads the correct positions from the BAG. It takes addresses that are evenly spread over the SQLite database
and compares their RD coordinates (`rd_x` and `rd_y`) with the BAG API of [PDOK](https://api.pdok.nl/kadaster/bag/ogc/v2).
They should be equal. It needs internet access, but not `pyproj`. A difference can also be caused by a change in the BAG
since the extract that you parsed. This is different from `validate_coordinates.py`, which checks the conversion to latitude and longitude.

-n, --count  
Number of addresses to compare (default: 20)

-d, --database  
SQLite database to check (default: the database in [config.py](config.py))

### [test_sqlite_db.py](test_sqlite_db.py)
Checks the SQLite database for info and errors. `import_bag.py` also performs these tests after parsing.

### [utils_sqlite_shrink.py](utils_sqlite_shrink.py)
Reduces the SQlite database size by removing BAG tables (nummers, verblijfsobjecten and its related tables, panden, ligplaatsen and standplaatsen) 
that are no longer needed due to the new 'adressen' table.
The parser also does this as a final step if `delete_no_longer_needed_bag_tables` is set to `True` in [config.py](config.py).

## Adressen table
An adres is a nevenadres if the `hoofd_nummer_id` field is set. It points to the `nummer_id` of the hoofdadres. 

## Coordinates
The BAG gives positions as Rijksdriehoekscoördinaten (RD), a flat map system of the Netherlands in meters. The parser keeps
these and also converts them to latitude and longitude. By default the latitude and longitude are in ETRS89, the European
standard, which you can use as WGS84 (the system of GPS, OpenStreetMap and Google Maps). You can also choose exact WGS84,
see [Options](#options).

### Fields
* `rd_x` and `rd_y`: the position in RD (meters), as in the BAG.
* `latitude` and `longitude`: the same position in degrees (ETRS89 by default, or WGS84, see [Options](#options)).
* `geometry`: only filled if `parse_geometries` is enabled in [config.py](config.py). The outline of the pand (in the
  adressen table), ligplaats, standplaats or woonplaats as JSON: a list of rings with [longitude, latitude] pairs, the
  order that GeoJSON uses.

### Where the position of an address comes from
* A verblijfsobject: its point. If the BAG has a polygon instead of a point, the centre (centroid) of the polygon.
* A ligplaats or standplaats: the first corner of its polygon.
* A nevenadres: the position of its hoofdadres.
* An address without a verblijfsobject, ligplaats or standplaats has no position (NULL).

### Conversion and accuracy
* The conversion uses the official transformation [RDNAPTRANS2018](https://www.nsgi.nl/rdnaptrans) of the NSGI, with
  `pyproj` and the correction grid in the [proj](proj) folder.
* It was validated against the 442 official GNSS stations of the Kadaster
  ([RDinfo](https://api.pdok.nl/kadaster/rdinfo/ogc/v1)): the difference is at most 0.3 millimeter.
* The RD positions that the parser reads from the BAG were compared with the BAG API of
  [PDOK](https://api.pdok.nl/kadaster/bag/ogc/v2) and were equal.

### Without pyproj
* The parser also works without `pyproj`. It then uses approximation equations by F.H. Schreutelkamp and G.L. Strang van Hees,
  with a constant correction of about 21 cm north and 18 cm east. This is accurate to about 7 cm on average and 23 cm at most,
  checked against the 442 official GNSS stations. For all 10 million addresses the difference with the official
  transformation is 6 cm on average and 23 cm at most. The parser shows a warning.
* The method is saved in the `bag_info` table of the database (`coordinate_method`). The database tests use it.
* What needs the official transformation needs `pyproj`: `coordinate_system = 'WGS84'` and validate_coordinates.py.

### ETRS89 and WGS84
* ETRS89 is the European reference system that RD is officially tied to. The European INSPIRE directive and EUREF
  recommend it for geographic data.
* ETRS89 is fixed to the Eurasian plate. WGS84 is fixed to the globe. They were equal in 1989. The difference in the
  Netherlands was about 88 cm on 1 January 2025 and grows about 2.4 cm per year. The ETRS89 coordinates of a point stay
  the same, the WGS84 coordinates of that point slowly change.
* By default the parser uses the ETRS89 latitude and longitude as WGS84, without a further transformation. The
  [Geonovum guideline](https://docs.geostandaarden.nl/crs/crs/) (section 3.3.2.2.1) describes this as treating WGS 84 as
  equal to ETRS89. It is suited for data from RD that is used for visualisation or combined with other data. PDOK, the
  open data platform of the Kadaster, also serves its ETRS89 and WGS84 coordinates as the same values.
* This is negligible for maps and apps. If you need exact WGS84 coordinates for a specific date, you can select it in [config.py](config.py).

### Options
Choose ETRS89 (default) or exact WGS84 with `coordinate_system` and `wgs84_epoch` in [config.py](config.py). The options are
explained there. The parser shows the coordinate system and the epoch in the log, for example
`coordinates: WGS84 for epoch 2026.784 (2026-10-12)`, and saves them in the `bag_info` table of the database
(`coordinate_system`, `wgs84_epoch`, `coordinate_method`, `parser_version` and `import_date`). The database tests use this table.

### Checks
* At startup the parser checks the conversion with 10 official GNSS stations. It stops with a clear message if something
  is wrong, for example if the grid file is missing.
* The database tests (`import_bag.py` and `test_sqlite_db.py`) check that the stored latitude and longitude match the
  conversion of the RD coordinates for a sample of addresses, and that all addresses are inside the Netherlands.
* [validate_coordinates.py](#validate_coordinatespy) validates the conversion against all official GNSS stations.
* [validate_coordinates_with_pdok.py](#validate_coordinates_with_pdokpy) compares positions of addresses with PDOK.

## Limitations and notes
* Coordinates: see the [Coordinates](#coordinates) section.
* verblijfsobjecten table:  
  A verblijfsobject can have multiple panden, gebruiksdoelen and nevenadressen. These are stored in the 
  `verblijfsobject_panden`, `verblijfsobject_gebruiksdoelen` and `verblijfsobject_nevenadressen` tables. 
  These tables, like the other BAG tables, are deleted if `delete_no_longer_needed_bag_tables` is set to `True`.
* Adressen table:  
  * Some `gebruiksdoelen` and `pand_ids` fields contain multiple, comma-separated, values. 
  * The bouwjaar and geometry field only contain the data of the first pand, even if an address has multiple panden. 
* There are probably several more things missing that I don't know about. Feel free to file a [GitHub issue](https://github.com/digitaldutch/BAG_parser/issues).

## Breaking changes
Changes that can break your queries or code. See the [changelog](CHANGELOG.md) for all changes.
* V102 (12 October 2026): The BAG parser now uses the `pyproj` package (`pip install -r requirements.txt`) for the conversion.
  Since V112 it uses approximation equations if `pyproj` is not installed.
  The latitude and longitude fields, and the geometry, are now calculated with the official RDNAPTRANS2018 transformation.
  They are about 0.3 meter different from earlier versions (up to 0.5 meter), because the earlier approximation equations
  were less accurate.
* V99 (11 October 2026): In the `adressen` table `pand_id` is renamed to `pand_ids` and `gebruiksdoel` to `gebruiksdoelen` (still comma-separated). 
  `pand_ids` now contains all panden of an address. The `verblijfsobjecten` table no longer has the `pand_id`, 
  `gebruiksdoel` and `nevenadressen` fields. They moved to the new `verblijfsobject_panden`, 
  `verblijfsobject_gebruiksdoelen` and `verblijfsobject_nevenadressen` tables. 
  The `gebruiksdoel` column in the CSV export is renamed to `gebruiksdoelen`. 

## Documents
* [Praktijkhandleiding BAG](https://imbag.github.io/praktijkhandleiding/) 
* [Catalogus Basisregistratie Adressen en Gebouwen](https://www.geobasisregistraties.nl/documenten/publicatie/2018/03/12/catalogus-2018)

## Tools

#### Official BAG viewer
The Kadaster has an online [BAG viewer](https://bagviewer.kadaster.nl/) where you can search any address or other info in the official database.

#### nlextract
This tool does not parse all data. If you need more data or professional support, buy it from [nlextract](https://nlextract.nl/),
who have a more complex, but also complete [parser](https://github.com/nlextract/NLExtract).

#### bagconv
Bert hubert has [written a parser](https://berthub.eu/articles/posts/dutch-postcode-and-building-database/) in C++,
[bagconv](https://github.com/berthubert/bagconv), which is quite similar to this one.

## License
This software is made available under the [MIT license](LICENSE).

## Acknowledgements
* The grid file `nl_nsgi_rdtrans2018.tif` in the [proj](proj) folder is part of the official coordinate transformation
  RDNAPTRANS2018. © NSGI (Nederlandse Samenwerking Geodetische Infrastructuur), converted to GeoTIFF and distributed by
  [PROJ-data](https://github.com/OSGeo/PROJ-data). License: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
  See [proj/README.txt](proj/README.txt).
* The official GNSS stations that are used to check the coordinate conversion come from RDinfo, open data of the
  Kadaster (license PDM 1.0, public domain), published on [PDOK](https://api.pdok.nl/kadaster/rdinfo/ogc/v1).
* The conversion is done with [pyproj](https://pyproj4.github.io/pyproj/) and the PROJ library.

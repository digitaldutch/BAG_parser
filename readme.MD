[![GitHub license](https://img.shields.io/github/license/digitaldutch/BAG_parser)](https://github.com/digitaldutch/BAG_parser/blob/master/LICENSE)
[![GitHub issues](https://img.shields.io/github/issues/digitaldutch/BAG_parser)](https://github.com/digitaldutch/BAG_parser/issues)

# Digital Dutch BAG parser

## TL;DR
Converts big, complex and hard to read XML Dutch addresses database (BAG or Basisregistratie Adressen en Gebouwen) into a 
user-friendly, file based, SQLite database by running a single Python script. 
No need to install any dependencies or a database server. 

Additional scripts will convert this SQLite database to for example CSV format. 

## Download the parsed BAG
If you don't want to run the script yourself, you can download the latest BAG in SQLite or CSV format from
our [releases](https://github.com/digitaldutch/BAG_parser/releases) section.

## About the BAG ##
The Dutch public addresses and buildings database (BAG or Basisregistratie Adressen en Gebouwen) is freely downloadable
from the Dutch cadastre agency named Kadaster. Hooray 🙂. 

The bad news is: The original BAG comes in a complex and hard to read XML format using thousands of zipped XML files, 
which will quickly temper your initial enthusiasm to quite low levels. 
It also does not include municipalities or provinces and provides coordinates using a system that non-experts won't 
expect named 
 [Rijksdriehoekscoördinaten](https://nl.wikipedia.org/wiki/Rijksdriehoeksco%C3%B6rdinaten)😲. 

## What this parser does ##
This Python utility parses the BAG database and converts it into a clean, easy to read & use SQLite database.
Municipalities (gemeenten) and provinces (provincies) are added. Rijksdriehoekscoördinaten coordinates are converted 
to standard WGS84 latitude and longitude coordinates. A few BAG bugs are fixed. 
Year of construction, floor area and intended use of buildings are also provided. 
Several tables (nummers, verblijfsobjecten, panden, ligplaatsen and standplaatsen) are merged into a general 'adressen'
table. The SQLite database can be used directly, or as a source to generate a *.csv file or update your own addresses 
databases. There are a couple of options available in the [config.py](config.py).

## Requirements ##
* Python 3.8 probably works, but 3.10 is 20% quicker.

## Usage ##
* Download, or better (because it allows easy updates) use git to download the BAG parser. Install parser:  
`git clone https://github.com/digitaldutch/BAG_parser`  
Update parser:
`git pull https://github.com/digitaldutch/BAG_parser`
* [Download the BAG (2.8 GB)](https://www.kadaster.nl/-/kosteloze-download-bag-2.0-extract) and save the file as `bag.zip` in the `input` folder.
* The [gemeenten.csv](input/gemeenten.csv) file is already included in the `input` folder, but you can [download the latest version from the CBS website](https://www.cbs.nl/nl-nl/onze-diensten/methoden/classificaties/overig/gemeentelijke-indelingen-per-jaar). Save it as `gemeenten.csv` in the input folder.
* Set your options in [config.py](config.py)
* Run `import_bag.py`
* Drink cocktails for 1 hour 🌴🍹😎 while watching the progress bar.
* Open the SQLite database with your favorite adminstration tool. I like [DBeaver](https://dbeaver.io/).
Here's an example query on SQLite database to get information about postcode 2514GL, huisnummer 78 (Paleis Noordeinde):
``` SQL
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
  a.oppervlakte                AS vloeroppervlakte,
  a.gebruiksdoel
FROM adressen a
  LEFT JOIN openbare_ruimten o ON a.openbare_ruimte_id = o.id
  LEFT JOIN gemeenten g        ON a.gemeente_id        = g.id
  LEFT JOIN woonplaatsen w     ON a.woonplaats_id      = w.id
  LEFT JOIN provincies p       ON g.provincie_id       = p.id
WHERE postcode = '2514GL'
  AND huisnummer = 68;
```
* When done parsing, use the [export_to_csv_postcodes.py](export_to_csv_postcodes.py) or [export_to_csv.py](export_to_csv.py) 
to create a *.csv file. These functions are easy to customize. I myself use one (not on GitHub yet, as it is customized) to pump the 
SQLite data into a live Firebird database.

## Python commands ##

### [import_bag.py](import_bag.py) ###
Parses the original BAG file and transforms it into a SQLite database. Takes about 1 hour. 1.5 hours if you switch on 
the `parse_geometries` option in the [config.py](config.py).

### [export_to_csv_postcodes.py](export_to_csv_postcodes.py) ###
Exports the addresses in SQLite database to a *.csv file with address info only. Takes about 30 seconds.

### [export_to_csv.py](export_to_csv.py) ###
Exports the addresses in SQLite database to a *.csv file including additional information, like year of construction, 
latitude, longitude, floor area and intended use of buildings. Takes about 75 seconds.

### [test_sqlite_db.py](test_sqlite_db.py) ###
Checks de SQLite database for info and errors. `import_bag.py` also performs these tests after parsing.

### [utils_sqlite_shrink.py](utils_sqlite_shrink.py) ###
Reduces the SQlite database size by removing BAG tables (nummers, verblijfsobjecten, panden, ligplaatsen and standplaatsen) 
that are no longer needed due to the new 'adressen' table.
The parser also does this as a final step if `delete_no_longer_needed_bag_tables` is set to True in [config.py](config.py).

## Limitations ##
* Only active addresses are included. History data is left out.
* Residence info (verblijfsobjecten) is only added to the main address (hoofdadres). Other addresses (nevenadres) are ignored.
* Probably several more things that I forgot. Let me know by filing a GitHub issue.
* The WGS84 coordinates are calculated using [approximation equations](docs/Benaderingsformules_RD_WGS.pdf) with a sub-decimeter accuracy.

Just a limited amount of data is parsed. If you need more data or professional support, buy it from [nlextract](https://nlextract.nl/), 
who have their own, much more complete [parser](https://github.com/nlextract/NLExtract).

## License ##
This software is made available under the [MIT license](LICENSE).
[![GitHub license](https://img.shields.io/github/license/digitaldutch/BAG_parser)](https://github.com/digitaldutch/BAG_parser/blob/master/LICENSE)
[![GitHub issues](https://img.shields.io/github/issues/digitaldutch/BAG_parser)](https://github.com/digitaldutch/BAG_parser/issues)

# Digital Dutch BAG parser

## TL;DR
Converts in a few minutes the big, complex and hard to read XML Dutch addresses database (BAG or Basisregistratie Adressen
en Gebouwen) into a user-friendly, file based, blazingly fast SQLite database by running a single Python script. 
No need to install any dependencies or a database server.

Additional scripts will convert this SQLite database to other formats, like CSV. 

## Download the parsed BAG
If you don't want to run the script yourself, download the latest BAG in SQLite or CSV format from
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
to standard WGS84 latitude and longitude coordinates. Invalid (dummy) bouwjaar and oppervlakte fields are removed. 
Year of construction, floor area and intended use of buildings are also provided. 
Several tables (nummers, verblijfsobjecten, panden, ligplaatsen and standplaatsen) are merged into a general 'adressen'
table. The SQLite database can be used directly, as a source to generate a *.csv file or to update your own addresses 
databases. There are a couple of options available in the [config.py](config.py).

## Requirements
* Python 3.13. Older Python versions may work, but are not tested and certainly slower.

## Usage
* Download or use git (recommended as updates are easier) to download the BAG parser.   
  Git command for initial checkout:  
  `git clone https://github.com/digitaldutch/BAG_parser`   
  Update to the latest version:  
  `git pull https://github.com/digitaldutch/BAG_parser`
* Download the BAG (3 GB) from [kadaster.nl](https://www.kadaster.nl/-/gratis-download-bag-extract)
  or directly from [pdok.nl](https://service.pdok.nl/kadaster/adressen/atom/v1_0/downloads/lvbag-extract-nl.zip)
  and save the file as `bag.zip` in the `input` folder.
* The [gemeenten.csv](input/gemeenten.csv) file is already included in the `input` folder, but you can [download the latest version from the CBS website](https://www.cbs.nl/nl-nl/onze-diensten/methoden/classificaties/overig/gemeentelijke-indelingen-per-jaar). Save it as `gemeenten.csv` in the input folder.
* Set your options in [config.py](config.py)
* Run `import_bag.py`
* Drink a cup of coffee for a few minutes ☕😎 while watching the progress bar.
* Open the SQLite database with your favorite tool. I like [DBeaver](https://dbeaver.io/).
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
  a.gebruiksdoel,
  a.hoofd_nummer_id
FROM adressen a
  LEFT JOIN openbare_ruimten o ON a.openbare_ruimte_id = o.id
  LEFT JOIN gemeenten g        ON a.gemeente_id        = g.id
  LEFT JOIN woonplaatsen w     ON a.woonplaats_id      = w.woonplaats_id
  LEFT JOIN provincies p       ON g.provincie_id       = p.id
WHERE postcode = '2514GL'
  AND huisnummer = 68;
```
* When done parsing, use [export_to_csv.py](export_to_csv.py) to create a *.csv file. This file has several command line options (see below). 
These conversion functions are easy to customize. 
I myself use one (not on GitHub) to pump the SQLite data into a live Firebird database.

## Python commands ##

### [import_bag.py](import_bag.py)
Parses the original BAG file and transforms it into a SQLite database. Takes about 6 minutes to complete
on an AMD 9900X PC, or a few minutes more if you switch on the `parse_geometries` option in the [config.py](config.py).

### [export_to_csv.py](export_to_csv.py)
Exports the addresses in SQLite database to a *.csv file. By default, only the addresses and
postcode data is exported (~15 seconds). Use the command options below for more output formats.  

-a, --all  
Export all data including year of construction, latitude, longitude, floor area and intended use of buildings. 
~40s

-h, --help  
show help message

-p4, --postcode4  
Export statistics of 4 character postal code groups. (e.g. 1000). ~10s 

-p5, --postcode5  
Export statistics of 5 character postal code groups (e.g. 1000A). ~10s

-p6, --postcode6  
Export statistics of 6 character postal code groups (e.g. 1000AA). ~10s

### [test_sqlite_db.py](test_sqlite_db.py)
Checks de SQLite database for info and errors. `import_bag.py` also performs these tests after parsing.

### [utils_sqlite_shrink.py](utils_sqlite_shrink.py)
Reduces the SQlite database size by removing BAG tables (nummers, verblijfsobjecten, panden, ligplaatsen and standplaatsen) 
that are no longer needed due to the new 'adressen' table.
The parser also does this as a final step if `delete_no_longer_needed_bag_tables` is set to `True` in [config.py](config.py).

## Adressen table
An adres is a nevenadres if the `hoofd_nummer_id` field is set. It points to the `nummer_id` of the hoofdadres. 

## Limitations and notes
* The WGS84 coordinates are calculated using [approximation equations by F.H. Schreutelkamp and G.L. Strang van Hees](docs/Benaderingsformules_RD_WGS.pdf). This conversion has an error of a few decimeters. Don't use the 
WGS84 coordinates if you need higher accuracy. 
* verblijfsobjecten table:  
  Some gebruiksdoel, pand_id and nevenadressen fields contain multiple,
  comma separated, values. Be careful if you do queries with joins on those fields.
* Adressen table:  
  * Some gebruiksdoel and pand_id fields contain multiple, comma separated, values.
  * The bouwjaar and geometry field only contain the data of one pand, even if an address has multiple panden. 
* There are probably several more things missing that I don't know about. Feel free to file a [GitHub issue](https://github.com/digitaldutch/BAG_parser/issues).

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

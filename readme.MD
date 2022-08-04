# Digital Dutch BAG parser

## TL;DR
Turns big, complex and hard to read XML Dutch addresses database (BAG or Basisregistratie Adressen en Gebouwen) into a 
user-friendly SQLite database by running a single Python script.

## Download BAG SQLite
The latest BAG in SQLite format can be downloaded from our [releases section](https://github.com/digitaldutch/BAG_parser/releases).

## Description ##

The Dutch public addresses and buildings database (BAG or Basisregistratie Adressen en Gebouwen) is freely downloadable
from the Dutch cadastre agency named Kadaster. Hooray 🙂. 

The bad news is: it comes in a complex and hard to read XML format using thousands of zipped XML files, 
which will quickly temper your initial enthusiasm to quite low levels. 
It also does not include municipalities or provinces and provides coordinates using an obscure system named 
 [Rijksdriehoekscoördinaten](https://nl.wikipedia.org/wiki/Rijksdriehoeksco%C3%B6rdinaten)😲. 

This Python utility parses the BAG database and turns it into a clean, easy to read & use SQLite database.
Municipalities (gemeenten) and provinces (provincies) are added. Coordinates are converted to standard latitude, 
longitude coordinates. Year of construction, floor area and intended use of buildings are also provided. Several 
tables (nummers, verblijfsobjecten, panden, ligplaatsen and standplaatsen) are merged into a general 'adressen'
table. The SQLite database can be used directly, or may be a source to generate a *.csv file or update your own addresses 
databases.  

## Requirements ##
* Python 3.8 works, but 3.10 is 20% quicker.

## Usage ##
* Download, or better, git clone this BAG parser.
* [Download the BAG (2.8 GB)](https://www.kadaster.nl/-/kosteloze-download-bag-2.0-extract) and save the file as `bag.zip` in the input folder.
* The gemeenten file is already included in the input folder, but you can [download the latest version from the CBS website](https://www.cbs.nl/nl-nl/onze-diensten/methoden/classificaties/overig/gemeentelijke-indelingen-per-jaar). Save it as `gemeenten.csv` in the input folder.
* Run `import_bag.py`
* Drink cocktails for 1 hour 🌴🍹😎 while watching the progress bar.
* Open the SQLite database with your favorite adminstration tool, in my case [DBeaver](https://dbeaver.io/).
Example query on SQLite database to get information about postcode 2514GL, huisnummer 78 (Paleis Noordeinde):
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

## Python commands ##

### import_bag.py ###
Parses the BAG file and transforms it into a SQLite database. Takes about an hour.

### test_sqlite_db.py ###
Checks de SQLite database for info and errors. `import_bag.py` also performs these tests after parsing.

### utils_sqlite_shrink.py ###
Reduces the SQlite database size by removing BAG tables (nummers, verblijfsobjecten, panden, ligplaatsen and standplaatsen) 
that are no longer needed due to the new 'adressen' table.
The parser also does this as a final step if `delete_no_longer_needed_bag_tables` is set to True in [config.py](config.py).

## Limitations ##
* Only active addresses are included. History data is left out.
* No geometry data is parsed, only coordinates.
* Residence info (verblijfsobjecten) is only added to the main address (hoofdadres). Other addresses (nevenadres) are ignored.
* Probably several more things that I forgot.

Just a limited amount of data is parsed. If you need more data or professional support, buy it from [nlextract](https://nlextract.nl/), 
who have their own, much more complete [parser](https://github.com/nlextract/NLExtract).

## License ##
This software is made available under the [MIT license](LICENSE).
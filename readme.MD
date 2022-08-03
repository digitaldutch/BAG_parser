# BAG parser

## TL;DR
Turns big, complex and hard to read XML Dutch addresses database (BAG or Basisregistratie Adressen en Gebouwen) into a 
user-friendly SQLite database by running a single Python script.

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
tables (nummers, verblijfsobjecten, panden, verblijfsobjecten and ligplaatsen) are merged into a general 'adressen'
table. The SQLite database can be used directly, or may be a source to generate a *.csv file or update your own addresses 
databases.  

## Limitations ##
Only active addresses are included. History data is left out. 
Residence info (verblijfsobjecten) is only added to the main address (hoofdadres). 
Other addresses (Nevenadres) are ignored.

Just a limited amount of data is parsed. If you need more data, buy it from [nlextract](https://nlextract.nl/), who
have their own, much more complete [parser](https://github.com/nlextract/NLExtract).


## Requirements ##
* Python 3.8 works, but 3.10 is 20% quicker.

## Usage ##
* [Download the BAG](https://www.kadaster.nl/zakelijk/producten/adressen-en-gebouwen/bag-2.0-extract) and save the file as 'bag.zip' in the input folder.
* The gemeenten file is already included in the input folder, but you can [download the latest version from the CBS website](https://www.cbs.nl/nl-nl/onze-diensten/methoden/classificaties/overig/gemeentelijke-indelingen-per-jaar). Save it as 'gemeenten.csv' in the input folder.
* Run `import_bag.py`
* Drink cocktails for 1 hour 🌴🍹😎 while watching the progress bar.

## Python commands ##

### import_bag.py ###
Parses the BAG file and transforms it into a SQLite database. Takes a little more than an hour on my PC.

### test_sqlite_db.py ###
Checks de SQLite database for info and errors. import_bag.py also performs these test after parsing.

### utils_sqlite_shrink.py ###
Reduces the SQlite database size by removing BAG tables (nummers, verblijfsobjecten, panden, verblijfsobjecten and ligplaatsen) 
that are no longer needed due to the new 'adressen' table.
The parser also does this as a final step if `delete_no_longer_needed_bag_tables` is set to True in [config.py](config.py).

## License ##
This software is made available under the [MIT license](LICENSE).
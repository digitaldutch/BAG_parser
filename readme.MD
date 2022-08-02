# BAG parser

## TL;DR
Turns ugly, huge and complex XML Dutch addresses database (BAG or Basisregistratie Adressen en Gebouwen) into a 
beautiful SQLite database. 
Download BAG, run a single Python script and go drink cappuccino's for 1 hour.

## Introduction ##

The Dutch public addresses and buildings database (BAG or Basisregistratie Adressen en Gebouwen) is freely downloadable
from the Dutch cadastre agency named Kadaster. Hooray 🙂. 

The bad news is: it comes in an ugly and unreadable XML format using thousands of zipped XML files, which will quickly 
temper your initial enthusiasm to quite a low level. 
It also does not include municipalities or provinces and provides coordinates using a system from the dark ages named 
 [Rijksdriehoekscoördinaten](https://nl.wikipedia.org/wiki/Rijksdriehoeksco%C3%B6rdinaten)😲. 
This is why several companies make a lot of money🤑 selling user friendlier versions of the BAG.

This Python utility parses the BAG database and turns it into a clean, easy to read & use SQLite database.
Municipalities (gemeenten) and provinces (provincies) are added. Coordinates are converted to standard latitude, 
longitude coordinates. Year of construction, floor area and intended use of buildings are also provided. Several 
tables (nummers, verblijfsobjecten, panden, verblijfsobjecten and ligplaatsen) are merged into a general addresses
table. Only active addresses are included.

The SQLite database can be used directly, or may be a source to generate a *.csv file or update your own addresses 
databases. 

## Requirements ##
* Python 3.8 works, but 3.10 is 20% quicker.

## Usage ##
* Read instructions in the [config.py](config.py) file. It tells you where to download and save the BAG file.
* Run `import_bag.py`

## Python commands ##

### import_bag.py ###
Parses the BAG file and transforms it into a SQLite database. Takes a little more than an hour on my PC.

### test_sqlite_db.py ###
Checks de SQLite database for info and errors. import_bag.py also performs these test after parsing.

### utils_sqlite_shrink.py ###
Reduces the SQlite database size by removing tables that are no longer needed BAG tables due to the new addresses table.

## License ##
This software is made available under the [MIT license](LICENSE).
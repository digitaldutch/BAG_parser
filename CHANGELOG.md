# Changelog

## V109 - 12 October 2026
* Without the optional `psutil` package the parser now uses the number of logical CPU cores (os.cpu_count) instead of a fixed
  8 cores. The warning shows the number that is used.
* On Windows the number of CPU cores used is limited to 61, the maximum of a process pool.

## V108 - 12 October 2026
* New option `coordinate_system` in config.py: `'ETRS89'` (default, as before) or `'WGS84'` for exact WGS84 coordinates with a
  time-dependent transformation. With `wgs84_epoch` you choose the epoch (date), by default the date of the import.
* The coordinate system and the epoch are shown in the log at the start of the import and saved in the new `bag_info` table
  of the database, together with the parser version and the import date. The database tests use this table.

## V107 - 12 October 2026
* New script validate_coordinates_with_pdok.py: compares the RD coordinates of addresses in the SQLite database with the BAG API
  of PDOK. This checks if the parser reads the correct positions from the BAG. Needs internet access, not pyproj.
* Readme: new Coordinates section that explains the coordinate fields, where the position of an address comes from, the conversion,
  ETRS89 and WGS84, and the checks. The credits for the grid file and the RDinfo data are in the new Acknowledgements section.

## V106 - 12 October 2026
* The startup check of the coordinate conversion now uses 10 official GNSS stations of the Kadaster (RDinfo, open data)
  as reference. These have official RD and ETRS89 coordinates. The maximum allowed difference is 1 cm (was 3 cm).
  The conversion was validated against all 442 GNSS stations of RDinfo: the maximum difference is 0.3 mm.
* New script validate_coordinates.py: downloads the GNSS stations from RDinfo and validates the conversion against all of
  them. With `--update-reference-points` it updates the table with reference points with the latest RDinfo data.
* Readme: the coordinates are now described as ETRS89 (the European standard), used as WGS84, with the background and the sources
  (Geonovum guideline, PDOK). The difference between ETRS89 and WGS84 is about 88 cm (1 January 2025) and grows 2.4 cm per year.
  The same is explained in the comments of coordinates.py, in the error messages and in requirements.txt.

## V105 - 12 October 2026
* The database test for addresses near the Lange Jan is replaced by a test that checks that all addresses are inside
  the borders of the Netherlands. This catches wrong coordinates for every address, for example swapped latitude and longitude.

## V104 - 12 October 2026
* Fixed: Verblijfsobjecten with a polygon geometry instead of a point had no coordinates (about 800 addresses).
  The location is now the centroid of the polygon.
* Fixed: Missing coordinates (rd_x, rd_y, latitude, longitude) are now NULL, not an empty text.

## V103 - 12 October 2026
* The parser now checks at startup if a newer version is available on GitHub and logs the result: a warning if there
  is a newer version, or a message that you are using the latest version.
  This makes one web request to GitHub. If GitHub can't be reached the check is skipped.
  Set `check_for_updates = False` in config.py to turn it off.

## V102 - 12 October 2026
* **Breaking:** Coordinates are now converted with the official RDNAPTRANS2018 transformation using the `pyproj` package
  and the NSGI correction grid (included in the new `proj` folder). This replaces the approximation equations.
  The latitude, longitude and geometry values change about 0.3 meter (up to 0.5 meter). This also closes #8.
* `pyproj` is now a required package. See Installation in the readme.
* The BAG parser checks the requirements at startup and shows clear error messages if `pyproj` or the grid file is missing
  or the conversion is not accurate. A missing `psutil` package now gives a warning with install instructions.
* New database tests: the stored latitude and longitude of a sample of addresses must match the official conversion of their RD coordinates (needs pyproj), and there must be addresses near the Lange Jan in Amersfoort.
* The readme has new Requirements, Installation and Running the parser sections
* Removed `bag/rijksdriehoek.py` and the document with the approximation equations

## V101 - 11 October 2026
* Fixed: The ligplaatsen and standplaatsen "zonder locatie" tests checked `gebruiksdoelen` and could never find anything. They now check `object_type`.
* Fixed: The "info: openbare ruimten" test line never showed as an error.
* Fixed: With `active_only = False` the `verblijfsobject_panden`, `verblijfsobject_gebruiksdoelen` and `verblijfsobject_nevenadressen` tables contained the values of all versions of a verblijfsobject. Now only the values of the version that is kept in `verblijfsobjecten` remain.

## V100 - 11 October 2026
* Python 3.11 or newer is now the documented requirement (was 3.14)
* Added requirements.txt. The only dependency, `psutil`, is optional.

## V99 - 11 October 2026
* **Breaking:** Fields that can have multiple values now have plural names and are stored properly (#19, #17):
  * `adressen`: `pand_id` is now `pand_ids` and `gebruiksdoel` is now `gebruiksdoelen`. Still comma-separated. 
    The `pand_ids` field now contains all panden of an address (before only one or none).
  * `verblijfsobjecten`: the comma-separated `pand_id`, `gebruiksdoel` and `nevenadressen` fields are removed. 
    They are now in the new tables `verblijfsobject_panden`, `verblijfsobject_gebruiksdoelen` and 
    `verblijfsobject_nevenadressen`. 
  * The `gebruiksdoel` column in the `export_to_csv.py -a` output is now named `gebruiksdoelen`.
* The adressen table is now created with SQL instead of Python loops

## V98 - 11 October 2026
* export_to_csv.py: new options `-o` / `--openbare-ruimten` to export all active openbare ruimten (also those without addresses) and `-w` / `--woonplaatsen` to export all active woonplaatsen (#28)

## V97 - 11 October 2026
* Fixed: Parsing inactive records (`active_only = False`) crashed on duplicate woonplaats IDs (#27). The woonplaats version with the latest begindatum_geldigheid is now kept.

## V96 - 11 October 2026
* Fall back to the system locale when `nl_NL` / `en_US` is not installed (config.py, statusbar/status.py)
* `.gitignore` now ignores `/scratch` instead of `/test`
* Readme: fixed example huisnummer (78)

## V95 - 30 July 2026
* Increased version number (just testing git)

## V94 - 27 July 2026
* Test adressen met ongeldige oppervlakten 99999 toont niet meer alle id's omdat dat er duizenden zijn
* Testen panden met ongeldig bouwjaar toont niet meer de ID's omdat dat er te veel zijn

## V93 - 10 March 2026
* Removed no longer used `_geometry` functions from utils.py
* Fixed: Pand geometry was not read correctly for some time. Pand uses 3D coordinates.
* Optimized coordinate conversion code

## V92 - 9 December 2025
* Woonplaatsen tabel now uses ID instead of woonplaats_id for id field.
* Changed openbareruimte_id field type to TEXT in addressen table. This fixes slow *.csv file generation in previous versions

## V90 - 7 December 2025
* Fixed: Geometry for verblijfsobjecten was not converted to WGS84 coordinates.
* Small tweaks

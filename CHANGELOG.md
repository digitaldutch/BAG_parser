# Changelog

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

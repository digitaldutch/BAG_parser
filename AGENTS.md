# AGENTS.md

Guidance for AI coding agents working on this repository.

## Project
Python (3.11 or newer) utility that parses the Dutch BAG (Basisregistratie Adressen en Gebouwen) XML extract from Kadaster into a
SQLite database, and exports it to CSV. Only `pyproj` is a required dependency (RD to WGS84 conversion); `psutil` is optional. Do not add other dependencies
without asking. See [README.md](README.md).

## Layout
* `import_bag.py` - main entry point: reads `input/bag.zip`, writes the SQLite database to `output/`.
* `export_to_csv.py` - exports the SQLite database to CSV.
* `test_sqlite_db.py` - data sanity checks on the generated SQLite database (run after an import; not a unit-test suite).
* `config.py` - user options, plus `version` and `version_date`.
* `bag/` - XML parsers (`bag_parser.py`, `gemeente_parser.py`) and RD to WGS84 conversion (`coordinates.py`, uses pyproj).
* `proj/` - the RDNAPTRANS2018 grid file (CC BY 4.0, see `proj/README.txt`) that makes the conversion accurate. Do not remove it.
* `database_sqlite/`, `exporter/`, `statusbar/` - database, CSV export and progress bar.
* `utils.py`, `logger.py` - shared helpers and logging (use `utils.print_log`).
* `input/` - `bag.zip` (gitignored, 3 GB) and `gemeenten.csv`. `output/`, `temp/`, `temp_xml/`, `scratch/` are gitignored.

## Conventions
* Match the surrounding code style and comment density. Database field names and log messages are Dutch where the
  existing ones are; code comments and docs are English.
* Pand geometry uses 3D coordinates; coordinates are converted from Rijksdriehoek (RD) to ETRS89, used as WGS84, with
  `bag/coordinates.py`. `import_bag.py` checks the requirements at startup (`coordinates.check_requirements()`).
* Invalid dummy values (bouwjaar, oppervlakte 99999) are removed on purpose; do not list thousands of ids in test output.
* Do not commit BAG data, SQLite or CSV output.

## Releasing
Every change that gets committed bumps the version:
1. Increase `version` and set `version_date` in [config.py](config.py) (format `'11 October 2026'`).
2. Add an entry at the top of [CHANGELOG.md](CHANGELOG.md) (`## V<n> - <date>`, bullet list).
3. Commit message format: `V<n> <date> * change one * change two`.

## Running
Needs `input/bag.zip`; a full import takes minutes. Prefer checking syntax / small isolated functions
over a full import unless asked.
```
python import_bag.py
python export_to_csv.py
python test_sqlite_db.py
```

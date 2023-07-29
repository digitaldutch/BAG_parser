import math
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
import os
import shutil
import sys
from zipfile import ZipFile

from logger import Logger
from bag import rijksdriehoek
from enum import Enum


class TextStyle(Enum):
    BLACK = '\033[30m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'
    UNDERLINE = '\033[4m'
    RESET = '\033[0m'


logger = Logger()


def unzip_files(zip_filename, filenames, path):
    with ZipFile(zip_filename, 'r') as file_zip:
        for filename in filenames:
            file_zip.extract(filename, path)


def unzip_files_multithreaded(zip_filename, path, workers_count=multiprocessing.cpu_count()):

    with ZipFile(zip_filename, 'r') as archive:
        files = archive.namelist()
        files_total = len(files)

    # It makes no sense to have:
    # - more workers than logical CPU cores
    # - more workers than jobs
    cpu_count = multiprocessing.cpu_count()
    workers_count = min([workers_count, cpu_count, files_total])

    # use ceil instead of round to prevent zero batch size
    batch_size = math.ceil(files_total / workers_count)

    with ProcessPoolExecutor(workers_count) as executor:
        for i in range(0, files_total, batch_size):
            filenames = files[i:(i + batch_size)]
            _ = executor.submit(unzip_files, zip_filename, filenames, path)


def clear_log():
    logger.clear()


def print_log(message, error=False):
    now = datetime.now()

    if error:
        message = 'ERROR: ' + message

    text = now.strftime("%Y-%m-%d %H:%M:%S.%f") + ' ' + message
    text_console = text
    if error:
        text_console = TextStyle.RED.value + text + TextStyle.RESET.value

    print(text_console)
    logger.log(text)


def find_file(folder, search_text, extension):
    for file_name in os.listdir(folder):
        if search_text in file_name and file_name.endswith(extension):
            return os.path.join(folder, file_name)


def find_xml_files(folder, search_text):
    files = []
    for file_name in os.listdir(folder):
        if search_text in file_name and file_name.endswith(".xml"):
            files.append(os.path.join(folder, file_name))

    return files


def empty_folder(folder):
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))


def print_progress_bar(i, max_value, text, final=False):
    bar_length = 10
    progress = i / max_value
    sys.stdout.write('\r')
    sys.stdout.write(f"[{'=' * int(bar_length * progress):{bar_length}s}] {int(100 * progress)}%  {text} ")
    sys.stdout.flush()
    if final:
        print('')


def bag_date_to_date(bag_date):
    if len(bag_date) >= 16:
        return datetime(year=int(bag_date[0:4]), month=int(bag_date[5:7]), day=int(bag_date[8:10]))
    else:
        return None

def bag_date_today():
    return datetime.today().strftime("%Y-%m-%d")


def bag_geometry_to_wgs_geojson(geometry):
    geometries = geometry.split(",")
    coordinates_wgs = ''
    for linear_ring in geometries:
        # Remove [] begin and end characters
        linear_ring = linear_ring[1:-1]
        linear_ring = linear_ring.split()
        ring_coordinates_wgs = ''
        it = iter(linear_ring)
        for x, y in zip(it, it):
            lat, lon = rijksdriehoek.rijksdriehoek_to_wgs84(float(x), float(y))
            if ring_coordinates_wgs:
                ring_coordinates_wgs += ','
            ring_coordinates_wgs += '[' + str(lon) + ',' + str(lat) + ']'

        if coordinates_wgs:
            coordinates_wgs += ','
        coordinates_wgs += '[' + ring_coordinates_wgs + ']'

    return coordinates_wgs


def bag_geometry_3d_to_wgs_geojson(coordinates_rd):
    coordinates_rd = coordinates_rd.split()
    coordinates_wgs = ''
    it = iter(coordinates_rd)
    for x, y, z in zip(it, it, it):
        lat, lon = rijksdriehoek.rijksdriehoek_to_wgs84(float(x), float(y))
        if coordinates_wgs != '':
            coordinates_wgs += ','
        coordinates_wgs += '[' + str(lon) + ',' + str(lat) + ']'

    coordinates_wgs = '[[' + coordinates_wgs + ']]'
    return coordinates_wgs


def bag_pos_to_rd_coordinates(pos):
    pos = pos.split()
    return float(pos[0]), float(pos[1])


def escape_sql_text(text):
    return text.replace("'", "''")

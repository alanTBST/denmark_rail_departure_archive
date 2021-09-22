# -*- coding: utf-8 -*-
"""
Created on Tue Dec  1 13:11:18 2020

@author: alkj
"""

import os
from pathlib import Path
import logging

log = logging.getLogger(__name__)

THIS_DIR = Path(__file__).parent
ARCHIVE_DIR = Path(THIS_DIR, 'archive')
TEMP_DIR = Path(THIS_DIR, 'temp_data')


REQUIRED_DEPARTURE_FILES = {
    'agency.txt',
    'calendar.txt',
    'routes.txt',
    'calendar_dates.txt',
    'stops.txt',
    'stop_times.txt',
    'trips.txt'
    }


ADDITIONAL_FILES = {
    'attributions.txt',
    'shapes.txt',
    'transfers.txt',
    'frequencies.txt',
    }

def validate_files() -> None:

    files_in_path = os.listdir(TEMP_DIR)
    if not all(x in files_in_path for x in REQUIRED_DEPARTURE_FILES):
        missing = set(files_in_path).symmetric_difference(
            REQUIRED_DEPARTURE_FILES) - ADDITIONAL_FILES
        log.error(f"Missing files - {''.join(map(str, missing))}")

    return


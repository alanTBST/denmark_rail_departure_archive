# -*- coding: utf-8 -*-
"""
Created on Wed Dec  2 14:56:54 2020

@author: alkj
"""

import os
from pathlib import Path
import json
import logging
from typing import Dict, Union

import pandas as pd

log = logging.getLogger(__name__)


T_STOPS = Dict[int, Dict[str, Union[str, int, float]]]


THIS_DIR = Path(__file__).parent
ARCHIVE_DIR = Path(THIS_DIR, 'archive')
TEMP_DIR = Path(THIS_DIR, 'temp_data')


def read_stops(filepath: Path) -> T_STOPS:

    df = pd.read_csv(filepath).fillna('')
    if not 'int' in df.loc[:, 'stop_id'].dtype.name:
        df.loc[:, 'stop_id'] = \
            df.loc[:, 'stop_id'].str.strip('G').astype(int)
        df = df.drop_duplicates('stop_id')

    return df.set_index('stop_id').T.to_dict()

def write_stops_to_archive(
        stop_dict: T_STOPS
        ) -> None:
    archive_loc = Path(ARCHIVE_DIR, 'stops.json')

    with open(archive_loc, 'w', encoding='iso-8859-1') as f:
        json.dump(stop_dict, f, indent=4)
    log.info("Stops data written to archive")


def read_archive_stops() -> T_STOPS:

    archive_loc = Path(ARCHIVE_DIR, 'stops.json')
    with open(archive_loc, 'r') as f:
        stops_archive = json.load(f)

    return {int(k):v for k, v in stops_archive.items()}


def find_dict_differences(archive: T_STOPS, new: T_STOPS) -> None:

    id_difference = set(archive) ^ set(new)
    new = {k: {k1:v1 for k1, v1 in v.items() if v != ''} for k, v in new.items()}
    archive = {k: {k1:v1 for k1, v1 in v.items() if v != ''} for k, v in archive.items()}

    for id_ in id_difference:
        if id_ not in archive:
            log.info(f"New Stop added: stop_id: {id_} '{new[id_]}'")
        elif id_ in new :
            log.info(f"Stop removed: stop_id: {id_} '{archive[id_]}'")

def check_stops() -> None:

    base_data = Path(THIS_DIR, 'base_data2', 'stops.txt')
    archive_data = Path(ARCHIVE_DIR, 'stops.json')
    new_data = Path(TEMP_DIR, 'stops.txt')

    if not os.path.isfile(archive_data):
        stops = read_stops(base_data)
        write_stops_to_archive(stops)

    archive_stops = read_archive_stops()
    new_stops = read_stops(new_data)
    find_dict_differences(archive_stops, new_stops)

    archive_stops.update(new_stops)
    write_stops_to_archive(archive_stops)


    return

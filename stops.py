# -*- coding: utf-8 -*-
"""
Created on Wed Dec  2 14:56:54 2020

@author: alkj
"""

import os
from pathlib import Path
import json
import logging
from typing import Dict, Optional, Union

import pandas as pd

log = logging.getLogger(__name__)


T_STOPS = Dict[int, Dict[str, Union[str, int, float]]]


THIS_DIR = Path(__file__).parent
ARCHIVE_DIR = Path(THIS_DIR, 'archive')
TEMP_DIR = Path(THIS_DIR, 'temp_data')


def read_stops(stops_filepath: Path) -> T_STOPS:
    """Read a given stops.txt file

    :param stops_filepath: path to the stops.txt file
    :type stops_filepath: Path
    :return: a dictionary with the stopid as the key. Other gtfs columns as an
        inner dictionary
    :rtype: T_STOPS
    """

    df = pd.read_csv(stops_filepath).fillna('')
    if not 'int' in df.loc[:, 'stop_id'].dtype.name:
        df.loc[:, 'stop_id'] = \
            df.loc[:, 'stop_id'].str.strip('G').astype(int)
        df = df.drop_duplicates('stop_id')

    return df.set_index('stop_id').T.to_dict()

def write_stops_to_archive(
        stop_dict: T_STOPS
        ) -> None:
    """Write a given stops dictionary into the stops.json file in the archive

    :param stop_dict: a dictionary of stops loaded using the read stops
        function
    :type stop_dict: T_STOPS
    """
    archive_loc =  ARCHIVE_DIR / 'stops.json'

    with open(archive_loc, 'w', encoding='iso-8859-1') as f:
        json.dump(stop_dict, f, indent=4)
    log.info("Stops data written to archive")


def read_archive_stops() -> T_STOPS:
    """Read the current stops.json file from the archive

    :return: a dictionary of the stop data
    :rtype: T_STOPS
    """

    archive_loc = ARCHIVE_DIR / 'stops.json'
    with open(archive_loc, 'r') as f:
        stops_archive = json.load(f)

    return {int(k):v for k, v in stops_archive.items()}


def find_dict_differences(
    archive_stops: T_STOPS,
    new_stops: T_STOPS
    ) -> None:
    """Find the differences between the stops in the archive and the new
    stops downloaded. Any stops added or removed are logged

    :param archive_stops: stop dict loaded from the archive
    :type archive_stops: T_STOPS
    :param new_stops: latest stop dict loaded from rejseplan
    :type new_stops: T_STOPS
    """

    id_difference = set(archive_stops) ^ set(new_stops)

    new = {
        k:  {k1:v1 for k1, v1 in v.items() if v != ''}
        for k, v in new_stops.items()
        }
    archive = {
        k: {k1:v1 for k1, v1 in v.items() if v != ''}
        for k, v in archive_stops.items()
        }

    for id_ in id_difference:
        if id_ not in archive:
            log.info(f"New Stop added: stop_id: {id_} '{new[id_]}'")
        elif id_ in new :
            log.info(f"Stop removed: stop_id: {id_} '{archive[id_]}'")

def check_stops(
    base_stops_path: Optional[Path] = None
    ) -> None:
    """Read in the new stop data that has been downloaded and
    compare it to the archive, if the archive exists.
    Then write the stops to the archive.

    :param base_stops_path: a path to a gtfs stops.txt file
        to start the archive with, defaults to None
    :type base_stops_path: Optional[Path], optional
    """

    if base_stops_path is not None:
        base_stops = read_stops(base_stops_path)
        write_stops_to_archive(base_stops)

    archive_stops_path = ARCHIVE_DIR / 'stops.json'
    new_data = TEMP_DIR / 'stops.txt'

    if archive_stops_path.is_file():
        archive_stops = read_archive_stops()
    else:
        archive_stops = {}

    new_stops = read_stops(new_data)
    find_dict_differences(archive_stops, new_stops)

    archive_stops.update(new_stops)
    write_stops_to_archive(archive_stops)


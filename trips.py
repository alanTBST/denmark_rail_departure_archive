# -*- coding: utf-8 -*-
"""
Created on Thu Dec  3 15:34:55 2020

@author: alkj
"""
from pathlib import Path
import logging
from typing import Dict, Union

import pandas as pd
import msgpack
import lmdb

from tools import find_date_range


log = logging.getLogger(__name__)

T_TRIPS = Dict[int, Dict[int, Dict[str, Union[str, int, float]]]]


THIS_DIR = Path(__file__).parent
ARCHIVE_DIR = Path(THIS_DIR, 'archive')
TEMP_DIR = Path(THIS_DIR, 'temp_data')

DB_SIZE = 1 * 1024 * 1024 * 1024


def read_trips(filepath: Path) -> T_TRIPS:
    """

    :param filepath: DESCRIPTION
    :type filepath: Path
    :return: DESCRIPTION
    :rtype: T_TRIPS

    """

    df = pd.read_csv(filepath, low_memory=False).fillna('')

    trip_routes = dict(zip(df.loc[:, 'trip_id'], df.loc[:, 'route_id']))

    trip_dict = df.set_index('trip_id').drop('route_id', axis=1).T.to_dict()
    return trip_routes, trip_dict

def write_trips_to_archive(
        trips: T_TRIPS,
        trip_dict,
        dates: str
        ) -> None:
    """

    :param trips: DESCRIPTION
    :type trips: T_TRIPS
    :param trip_dict: DESCRIPTION
    :type trip_dict: TYPE
    :param dates: DESCRIPTION
    :type dates: str
    :return: DESCRIPTION
    :rtype: None

    """

    archive_loc =str(Path(ARCHIVE_DIR, dates))

    with lmdb.open(archive_loc, map_size=DB_SIZE, max_dbs=5) as env:
        child_db = env.open_db(bytes('trip_route', 'utf-8'))
        with env.begin(db=child_db, write=True) as txn:
            for k, v in trips.items():
                key = bytes(str(k), 'utf-8')
                value = bytes(str(v), 'utf-8')
                txn.put(key, value)
    log.info(f"Trip/Routes data written to archive in : {dates}")

    with lmdb.open(archive_loc, map_size=DB_SIZE, max_dbs=5) as env:
        child_db = env.open_db(bytes('trips', 'utf-8'))
        with env.begin(db=child_db, write=True) as txn:
            for k, v in trip_dict.items():
                key = bytes(str(k), 'utf-8')
                value = msgpack.packb(v)
                txn.put(key, value)
    log.info(f"Trips data written to archive in : {dates}")

def check_trips() -> None:
    """

    :return: DESCRIPTION
    :rtype: None

    """

    new_data = Path(TEMP_DIR, 'trips.txt')


    new_dates = find_date_range(TEMP_DIR)
    new_trip_route, trips = read_trips(new_data)
    write_trips_to_archive(new_trip_route, trips, new_dates)

    return

# -*- coding: utf-8 -*-
"""
Created on Tue Dec  1 14:28:34 2020

@author: alkj
"""

import json
from pathlib import Path
from itertools import chain
from typing import Optional, Dict, Union

import lmdb
import msgpack
import pandas as pd

THIS_DIR = Path(__file__).parent
ARCHIVE_DIR = Path(THIS_DIR, 'archive')
TEMP_DIR = Path(THIS_DIR, 'temp_data')

DB_SIZE = 1 * 1024 * 1024 * 1024



def find_date_range(dirpath: Optional[Path] = None) -> str:
    """find the date range from the calendar.txt gtfs data"""

    if not dirpath:
        dirpath = Path(TEMP_DIR)
    cal_fp = Path(dirpath, 'calendar.txt')
    dates = pd.read_csv(cal_fp).iloc[0, [-2, -1]].values

    return f'{min(dates)}_{max(dates)}'


def load_config():
    fp = Path(THIS_DIR, 'config.json')

    with open(fp, 'r') as f:
        return json.load(f)

def load_stop_location_url():

    fp = Path(THIS_DIR, 'config.json')

    with open(fp, 'r') as f:
        return json.load(f)['location_url']

def load_bus_maps():

    fp = Path(THIS_DIR, 'bus_to_station_maps.json')
    with open(fp, 'r') as f:
        bus_map = json.load(f)
        return {int(k): v for k, v in bus_map.items()}



def filter_last_stop(stoptimes):
    out = {}
    for k, v in stoptimes.items():
        nstops = max(v)
        out[k] = {k1: v1 for k1, v1 in v.items() if k1 != nstops}

    return out

class ArchiveStore:

    def __init__(self, dates: Optional[str] = None) -> None:
        """
        A class to access the GTFS archive made at TBST
        :param dates: the date range string of the archive, defaults to None
        :type dates: Optional[str], optional
        :rtype: None

        """

        self.dates = dates


    def load_agency():
        fp = Path(ARCHIVE_DIR, 'agency.json')
        with open(fp, 'r') as f:
            return json.load(f)

    @staticmethod
    def load_routes() -> Dict[str, Dict[str, Union[int, str]]]:

        fp = Path(ARCHIVE_DIR, 'routes.json')
        with open(fp, 'r') as f:
            return json.load(f)

    @staticmethod
    def load_stops():

        fp = Path(ARCHIVE_DIR, 'stops.json')
        with open(fp, 'r') as f:
            return json.load(f)

    @staticmethod
    def load_trip_route(dates: Optional[str] = None, ) -> Dict[int, str]:
        """
        Load the mapping of trip_id -> route_id
        :param dates: , defaults to None
        :type dates: Optional[str], optional
        :return: dictionary of trip_ids to route_ids
        :rtype: Dict[int, str]

        """

        if dates is None:
            dates = find_date_range()

        archive_loc = str(Path(ARCHIVE_DIR, dates))

        out = {}
        with lmdb.open(archive_loc, map_size=DB_SIZE, max_dbs=5) as env:
            child_db = env.open_db(bytes('trip_route', 'utf-8'))
            with env.begin(db=child_db) as txn:
                cursor = txn.cursor()
                for k, v in cursor:
                    out[int(k.decode('utf-8'))] = v.decode('utf-8')
        return out

    @staticmethod
    def load_calendar(dates=None):
        """

        :param dates: DESCRIPTION, defaults to None
        :type dates: TYPE, optional
        :return: DESCRIPTION
        :rtype: TYPE

        """

        if dates is None:
            dates = find_date_range()

        archive_loc = str(Path(ARCHIVE_DIR, dates))

        fp = Path(archive_loc, 'calendar.json')
        with open(fp, 'r') as f:
            cal = json.load(f)
        return {int(k): v for k, v in cal.items()}

    @staticmethod
    def load_calendar_dates(dates=None):
        """

        :param dates: DESCRIPTION, defaults to None
        :type dates: TYPE, optional
        :return: DESCRIPTION
        :rtype: TYPE

        """


        if dates is None:
            dates = find_date_range()

        archive_loc = str(Path(ARCHIVE_DIR, dates))

        fp = Path(archive_loc, 'calendar_dates.json')
        with open(fp, 'r') as f:
            cal_exceptions = json.load(f)

        return {int(k): tuple(tuple(x) for x in v) for k, v in cal_exceptions.items()}

    @staticmethod
    def load_stop_times(
            dates: Optional[str] = None,
            **kwargs
            ) -> Dict[int, Dict[int, Dict[str, Union[str, int]]]]:

        pickup_type = kwargs.pop('pickup_type', None)

        if dates is None:
            dates = find_date_range()

        archive_loc = str(Path(ARCHIVE_DIR, dates))

        out = {}
        with lmdb.open(archive_loc, map_size=DB_SIZE, max_dbs=5) as env:
            child_db = env.open_db(bytes('stop_times', 'utf-8'))
            with env.begin(db=child_db) as txn:
                cursor = txn.cursor()
                for k, v in cursor:
                    val = msgpack.unpackb(v, strict_map_key=False)
                    if pickup_type is not None:
                        val = {
                            key: value for
                            key, value in val.items() if value['pickup_type'] == pickup_type
                            }
                    out[int(k.decode('utf-8'))] = val

        return out
    @staticmethod
    def load_trips(dates=None):

        if dates is None:
            dates = find_date_range()

        archive_loc = str(Path(ARCHIVE_DIR, dates))

        out = {}
        with lmdb.open(archive_loc, map_size=DB_SIZE, max_dbs=5) as env:
            child_db = env.open_db(bytes('trips', 'utf-8'))
            with env.begin(db=child_db) as txn:
                cursor = txn.cursor()
                for k, v in cursor:
                    out[int(k.decode('utf-8'))] = msgpack.unpackb(v, strict_map_key=False)

        return out
    @classmethod
    def trip_agency_map(cls, dates=None):

        routes = cls.load_routes()
        triproutes = cls.load_trip_route(dates=dates)

        return {k: routes[v]['agency_id'] for k, v in triproutes.items()}

    @classmethod
    def trip_service_map(cls, dates=None):

        trips = cls.load_trips(dates=dates)

        return {k: v['service_id'] for k, v in trips.items()}


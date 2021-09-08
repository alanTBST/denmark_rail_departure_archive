# -*- coding: utf-8 -*-
"""
Created on Fri Dec  4 10:17:05 2020

@author: alkj
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, NamedTuple, Type, TypeVar, Set, Dict, Generator

import pandas as pd
from tqdm import tqdm

from busstops import load_bus_maps
from tools import find_date_range, ArchiveStore
from rejsekortcollections import mappers as MAPPERS


METRO_MAP = MAPPERS['metro_map']

THIS_DIR = Path(__file__).parent
ARCHIVE_DIR = Path(THIS_DIR, 'archive')
TEMP_DIR = Path(THIS_DIR, 'temp_data')


T = TypeVar('T', bound='DepartureRecord')
DATE = pd._libs.tslibs.timestamps.Timestamp

class DepartureRecordBase(NamedTuple):


    stop_id: int
    departure_hour: int
    agency: int
    date: datetime

class DepartureRecord(DepartureRecordBase):


    def __new__(
            cls: Type[T],
            stop_id: int,
            departure_hour: int,
            agency: int,
            date: datetime
            ) -> T:
        """

        :param cls: DepartureRecord
        :type cls: Type[T]
        :param stop_id: stop_id of the bus stop or station
        :type stop_id: int
        :param departure_hour: the base hour or the departure time
        :type departure_hour: int
        :param agency: the agency_id  providing the departure
        :type agency: int
        :param date: the date of the departure
        :type date: datetime
        :return: an instance of a Departure Record
        :rtype: T

        """

        new_date = validate_date(departure_hour, date).date()
        if new_date == date:
            new_departure_hour = departure_hour
        else:
            new_departure_hour = departure_hour - 24
        return super().__new__(cls, stop_id, new_departure_hour, agency, new_date)

def make_date_range(daterange_string: str) -> Tuple[DATE, ...]:
    """
    Given a daterange string in the format 'YYYYMMDD_YYYYMMDD',
    create a tuple of all dates from the first date to the second date

    :param daterange_string: a string of the daterange ('YYYYMMDD_YYYYMMDD')
    :type daterange_string: str
    :return: tuple of all dates in the range
    :rtype: Tuple[DATE, ...]

    """

    start, end = daterange_string.split('_')

    return tuple(pd.date_range(start, end, freq='D'))

def filter_last_stop(stoptimes):
    """

    :param stoptimes: DESCRIPTION
    :type stoptimes: TYPE
    :return: DESCRIPTION
    :rtype: TYPE

    """

    out = {}
    for k, v in stoptimes.items():
        nstops = max(v)
        if nstops == 0:
            out[k] = v
            continue
        out[k] = {k1: v1 for k1, v1 in v.items() if k1 != nstops}

    return out

def _find_changed_services(
        date_int: int,
        calendar_dates
        ) -> Tuple[Set[int], Set[int]]:
    """

    :param date_int: DESCRIPTION
    :type date_int: int
    :param calendar_dates: DESCRIPTION
    :type calendar_dates: TYPE
    :return: DESCRIPTION
    :rtype: Tuple[Set[int], Set[int]]

    """


    added = set()
    removed = set()
    for service_id, exceptions in calendar_dates.items():
        date_exceptions = tuple(filter(lambda x: x[0] == date_int, exceptions))
        for exception in date_exceptions:
            if exception[1] == 1:
                added.add(service_id)
            elif exception[1] == 2:
                removed.add(service_id)
    return added, removed


def validate_date(departure_hour: int, date: datetime) -> datetime:
    """

    :param departure_hour: The hour of departure
    :type departure_hour: int
    :param date: The date of the departure
    :type date: datetime
    :return: Updated date if after midnight
    :rtype: datetime

    """

    if departure_hour > 23:
        return date + pd.Timedelta(1, unit='D')

    return date

def create_day_schedule(
        date: DATE,
        calendar,
        trips_service, calendar_dates,
        stop_times,
        trip_agency
        ) -> Generator[DepartureRecord, None, None]:
    """

    :param date: DESCRIPTION
    :type date: DATE - pandas Timestamp
    :param calendar: DESCRIPTION
    :type calendar: TYPE
    :param trips_service: DESCRIPTION
    :type trips_service: TYPE
    :param calendar_dates: DESCRIPTION
    :type calendar_dates: TYPE
    :param stop_times: DESCRIPTION
    :type stop_times: TYPE
    :param trip_agency: DESCRIPTION
    :type trip_agency: TYPE
    :yield: a single DepartureRecord
    :rtype: Generator

    """

    weekday = date.strftime("%A").lower()
    date_int = int(date.strftime('%Y%m%d'))

    services_for_weekday = {
        k for k, v in calendar.items() if v[weekday] == 1
        }
    added, removed = _find_changed_services(date_int, calendar_dates)
    final_services = services_for_weekday.union(added) - removed
    trips_for_date = {
        k for k, v in trips_service.items() if v in final_services
        }

    stop_times_date = {
        k: v for k, v in stop_times.items() if k in trips_for_date
        }

    for k, v in stop_times_date.items():
        agency = trip_agency[k]
        for stop_info in v.values():
            departure_hour = int(stop_info['departure_time'].split(':')[0])
            stop_id = stop_info['stop_id']

            record = DepartureRecord(
                stop_id=stop_id,
                departure_hour=departure_hour,
                agency=agency,
                date=date
                )
            yield record

def _load_bus_closest_station_stops() -> Dict[int, int]:
    """

    :return: DESCRIPTION
    :rtype: Dict[int, int]

    """

    fp = Path(THIS_DIR, 'bus_station_map.json')
    with open(fp, 'r') as f:
        maps = json.load(f)
        return {int(k): v for k, v in maps.items()}


LETBANE = {
    860012901, 860014801, 860014901, 860015101, 860015201,
    860012902, 860014802, 860014902, 860015102,	860015202,
    860430301, 860430401, 860430501, 860430601,	860430701, 
    860430801, 860430901, 860431001, 860431101, 860431201,	
    860431301, 860431401, 860431501, 860431601,	860431701,	
    860431801, 860015201, 860430302, 860430402,	860430502,	
    860430602, 860430702, 860430802, 860430902,	860431002,	
    860431102, 860431202, 860431302, 860431402,	860431502,	
    860431601, 860431702, 860431802, 860015202
    }

def _period_departures(dates: str) -> Tuple[pd.core.frame.DataFrame, pd.core.frame.DataFrame]:
    """
    
    :return: DESCRIPTION
    :rtype: TYPE

    """

    trips_service = ArchiveStore.trip_service_map(dates)
    trip_agency = ArchiveStore.trip_agency_map(dates)

    stop_times = ArchiveStore.load_stop_times(dates, pickup_type=0)
    stop_times = filter_last_stop(stop_times)

    calendar = ArchiveStore.load_calendar(dates)
    calendar_dates = ArchiveStore.load_calendar_dates(dates)
    date_range = make_date_range(dates)

    rail = []
    bus = []
    for date in tqdm(
            date_range,
            f'find departures for {date_range[0].date()} to {date_range[-1].date()}'
            ):

        day_schedule = create_day_schedule(
            date, calendar,
            trips_service,
            calendar_dates,
            stop_times,
            trip_agency
            )
        for x in day_schedule:
            if (7400000 < x.stop_id < 8700000) or x.stop_id in LETBANE:
                rail.append(x)
            else:
                bus.append(x)

    return pd.DataFrame(rail), pd.DataFrame(bus)


def _process_frame_for_output(
        frame: pd.core.frame.DataFrame,
        agency_map: Dict[int, str]
        ) -> pd.core.frame.DataFrame:
    """

    :param rail_frame: dataframe of rail departures returned
     from _period_departures
    :type rail_frame: pd.core.frame.DataFrame
    :param agency_map: dict mapping of agency id to agency name
    :type agency_map: Dict[int: str]
    :return: aggregated results of departures at rail stations per hour
    :rtype: pd.core.frame.DataFrame

    """

    frame.loc[:, 'agency'] = \
        frame.loc[:, 'agency'].replace(agency_map)

    frame = frame.groupby(list(frame.columns)).size()
    frame.name = 'count'
    frame = frame.reset_index()
    frame = frame.pivot_table(
        columns='agency',
        index=['date', 'stop_id', 'departure_hour'],
        values=['count'],
        aggfunc='max',
        fill_value=0
        ).reset_index()

    frame.columns = [
        '_'.join(str(s).strip() for s in col if s) for
        col in frame.columns
        ]

    new_cols = []
    for col in frame.columns:
        if not 'count' in col:
            new_cols.append(col)
        else:
            new_cols.append(col.split('_')[1])

    frame.columns = new_cols

    # change columns to match dwh
    frame = frame.rename(columns={'stop_id': 'station',
                                  'departure_hour': 'hour'})

    return frame


def _set_totals(df, col_name='total'):

    """

    :param df: DESCRIPTION
    :type df: TYPE
    :param col_name: DESCRIPTION, defaults to 'total'
    :type col_name: TYPE, optional
    :return: DESCRIPTION
    :rtype: TYPE

    """

    operator_columns = [
        x for x in df.columns if  x not in
        ('station', 'hour', 'date', 'stop_id', 'departure_hour')
        ]
    df.loc[:, col_name] = df.loc[:, operator_columns].sum(axis=1)

    return df

def calculate_departures(
        dates: str
        ) -> Tuple[pd.core.frame.DataFrame, pd.core.frame.DataFrame]:
    """
    :return: DESCRIPTION
    :rtype: TYPE

    """


    rail, bus = _period_departures(dates)

    # ensure only the parent station has results
    rail.loc[:, 'stop_id'] = rail.loc[:, 'stop_id'].replace(METRO_MAP)
    bus.loc[:, 'stop_id'] = bus.loc[:, 'stop_id'].replace(METRO_MAP)

    bus_maps =  load_bus_maps()
    bus = bus.query("stop_id in @bus_maps") # only stops that have stations
    bus.loc[:, 'stop_id'] = bus.loc[:, 'stop_id'].map(bus_maps)

    agency = ArchiveStore.load_agency()
    agency_map = {x['agency_id']: x['agency_name'] for x in agency.values()}

    rail_frame = _process_frame_for_output(rail, agency_map)
    bus_frame = _process_frame_for_output(bus, agency_map)

    rail_frame = _set_totals(rail_frame, col_name='total')
    bus_frame = _set_totals(bus_frame, col_name='total_bus')

    bus_frame.loc[:, 'date'] = \
        bus_frame.loc[:, 'date'].apply(lambda x: int(x.strftime('%Y%m%d')))

    rail_frame.loc[:, 'date'] = \
        rail_frame.loc[:, 'date'].apply(lambda x: int(x.strftime('%Y%m%d')))

    return rail_frame, bus_frame

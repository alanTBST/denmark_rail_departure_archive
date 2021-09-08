# -*- coding: utf-8 -*-
"""
Created on Thu Dec 10 14:48:48 2020

@author: alkj
"""

import copy
import json
import logging
import math
import urllib.request
import xml.etree.ElementTree as ET
from typing import Dict, Union, Tuple, Optional, Generator
from pathlib import Path
from urllib.error import HTTPError

import pandas as pd
from geopy.distance import geodesic

from stops import read_archive_stops
from tools import load_config, load_bus_maps
from rejsekortcollections import mappers as MAPPERS



THIS_DIR = Path(__file__).parent

METRO_MAPS = MAPPERS['metro_map']
METRO_MAPS_REV = MAPPERS['metro_map_rev']

log = logging.getLogger(__name__)


ALIASES = {
    'københavn h': ('hovedbanegården', 'hovedbanegård'),
    'cph lufthavn': ('københavns lufthavn', 'kastrup lufthavn')
    }


EXCEPTIONS = {
    'taastrup st.': 'høje taastrup'
    }


def _load_api_urls() -> Tuple[str, str]:
    """
    Load the Rejseplan API urls for locations and stops nearby

    :return: the urls from the config file
    :rtype: Tuple[str, str]

    """


    urls = load_config()
    location = urls['location_url']
    stops_nearby = urls['stops_nearby_url']
    return location, stops_nearby


def _get_stop_attributes(url: str) -> Dict[int, Union[str, int]]:
    """

    :param url: rejseplan location url
    :type url: str
    :return: a dictionary of attributes for the location request
    :rtype: Dict[int, Union[str, int]]

    """

    opener = urllib.request.build_opener()
    out = {}
    with opener.open(url) as file:
       root = ET.parse(file).getroot()
       for child in root.findall('StopLocation'):
           attr = child.attrib
           out[int(attr['id'])] = {
               'x': int(attr['x']),
               'y': int(attr['y']),
               'name': attr['name']
               }

    return out

def _get_nearby_stops(url: str) -> Dict[int, Union[str, int]]:
    """
    Return the API results
    :param url: stops nearby Rejseplan API url
    :type url: str
    :return: dictionary of the stops matching the nearby request
    :rtype: Dict[int, Union[str, int]]

    """

    opener = urllib.request.build_opener()
    out = {}
    with opener.open(url) as file:
        root = ET.parse(file).getroot()
        for child in root.findall('StopLocation'):
            attr = child.attrib
            out[int(attr['id'])] = {
                'name': attr['name'],
                'distance': int(attr['distance']),
                'x': int(attr['x']),
                'y': int(attr['y']),
                }
    if not out:
        raise ValueError("Not a current station/stop")

    return out

def stops_near_station(
        station_uic: int,
        radius: int
        ) -> Dict[int, Dict[str, Union[int, str]]]:
    """

    :param station_uic: DESCRIPTION
    :type station_uic: int
    :param radius: DESCRIPTION
    :type radius: int
    :return: DESCRIPTION
    :rtype: Dict[int, Dict[str, Union[int, str]]]

    """


    location_url, stops_nearby_url = _load_api_urls()
    loc = location_url.format(station_uic)
    stop_attributes = _get_stop_attributes(loc)

    x = stop_attributes[station_uic]['x']
    y = stop_attributes[station_uic]['y']

    nearby_url = stops_nearby_url.format(x, y, radius)
    try:
        stops_nearby = _get_nearby_stops(nearby_url)
        return {k: v for k, v in stops_nearby.items() if k != station_uic}
    except HTTPError:
        log.warning(f"API Server request problem: {station_uic}")

    except ValueError:
        log.warning(f"Bad value called to stops_near_station: {station_uic}")


def calculate_distance(
        stop_id_1: int,
        stop_id_2: int,
        stops_data
        ) -> float:

    loc1 = stops_data[stop_id_1]
    location1 = loc1['stop_lat'], loc1['stop_lon']

    loc2 = stops_data[stop_id_2]
    location2 = loc2['stop_lat'], loc2['stop_lon']

    return geodesic(location1, location2).meters

def _stop_coordinates(info_dict):

    return info_dict['stop_lat'], info_dict['stop_lon']

def _location_tolerance(
        rail_loc: Tuple[float, float],
        bus_loc: Tuple[float, float],
        e: Optional[float] = 0.01
        ) -> bool:
    """

    :param rail_loc: DESCRIPTION
    :type rail_loc: Tuple[float, float]
    :param bus_loc: DESCRIPTION
    :type bus_loc: Tuple[float, float]
    :param e: DESCRIPTION, defaults to 0.01
    :type e: Optional[float], optional
    :return: DESCRIPTION
    :rtype: bool

    """

    diffs = (abs(x - bus_loc[i]) for i, x in enumerate(rail_loc))

    biggest = max(diffs)

    return biggest < e

def _check_location_tolerance(
        partial: pd.core.frame.DataFrame,
        rail_coords: Tuple[float, float]
        ) -> Generator[int, None, None]:

    bus_stops = zip(partial.index, partial.stop_lat, partial.stop_lon)
    for bus_stop in bus_stops:
        if _location_tolerance(rail_coords, bus_stop[1:]):
            yield bus_stop[0]


def _process_bus_frame_for_matching(
        bus_frame: pd.core.frame.DataFrame
        ) -> pd.core.frame.DataFrame:

    bus_frame.loc[:, 'stop_name'] = bus_frame.loc[:, 'stop_name'].str.lower()
    bus_frame.loc[:, 'stop_name'] = \
        bus_frame.loc[:, 'stop_name'].str.lstrip().str.rstrip()
    bus_frame.loc[:, 'stop_name'] = \
        bus_frame.loc[:, 'stop_name'].str.replace('.', '')

    return bus_frame



def _partial_query(
        df: pd.core.frame.DataFrame,
        text: str
        ) -> pd.core.frame.DataFrame:

    return df[df.loc[:, 'stop_name'].str.contains(fr'\b{text}\b',  regex=True)]


def _match_exceptions(name, df):
    partial = df[
        (df.loc[:, 'stop_name'].str.contains(name, regex=False)) &
        ~(df.loc[:, 'stop_name'].str.contains(
            EXCEPTIONS[name], regex=False
            ))
        ]
    return partial

def _match_aliases(name, df):
    partial = pd.DataFrame()
    names = ALIASES[name]
    for name in names:
        part =  _partial_query(df, name)
        partial = pd.concat([partial, part])

    return

def get_matches(rail_stops, bus_frame: pd.core.frame.DataFrame):
    """

    :param rail_stops: DESCRIPTION
    :type rail_stops: TYPE
    :param bus_frame: DESCRIPTION
    :type bus_frame: TYPE
    :return: DESCRIPTION
    :rtype: TYPE

    """
    bus_frame = _process_bus_frame_for_matching(bus_frame)

    matches = {}
    for stop_id, info in rail_stops.items():

        idxs = set()

        rail_coords = _stop_coordinates(info)
        name = info['stop_name'].lower().rstrip('.') # strip '.' for regex in partial query

        partial =  _partial_query(bus_frame, name)

        valid_bus_stop_ids = set(
            _check_location_tolerance(partial, rail_coords)
            )

        idxs.update(valid_bus_stop_ids)

        if name in EXCEPTIONS:
            partial = bus_frame[
                (bus_frame.loc[:, 'stop_name'].str.contains(name, regex=False)) &
                ~(bus_frame.loc[:, 'stop_name'].str.contains(
                    EXCEPTIONS[name], regex=False
                    ))
                ]
        valid_bus_stop_ids = set(
            _check_location_tolerance(partial, rail_coords)
            )
        idxs.update(valid_bus_stop_ids)

        if name in ALIASES:
            partial = pd.DataFrame()
            names = ALIASES[name]
            for name in names:
                part =  _partial_query(bus_frame, name)
                partial = pd.concat([partial, part])

        valid_bus_stop_ids = set(
            _check_location_tolerance(partial, rail_coords)
            )
        idxs.update(valid_bus_stop_ids)

        matches[stop_id] = idxs

    return matches


def _roundup_to_ten(x):
    return int(math.ceil(x / 10.0)) * 10


def _all_nearby_stops(max_distances):

    out = {}
    for k, v in max_distances.items():
        v = _roundup_to_ten(v)
        try:
            nearby = stops_near_station(k, v)
            out[k] = nearby
        except Exception as e:
            log.warning(f"No result found for {k} - {e.__class__.__name__}")
    return out

def _calculate_match_distances(matches, stops_archive, default_distance=200):
    """

    :param matches: DESCRIPTION
    :type matches: TYPE
    :param stops_archive: DESCRIPTION
    :type stops_archive: TYPE
    :param default_distance: DESCRIPTION, defaults to 200
    :type default_distance: TYPE, optional
    :return: DESCRIPTION
    :rtype: TYPE

    """


    max_station_distance = {}
    for station_uic, bus_stops in matches.items():
        if bus_stops:
            d = max(
                calculate_distance(station_uic, stop, stops_archive)
                for stop in bus_stops
                )

            d = d if d > default_distance else default_distance
        else:
            d = default_distance

        max_station_distance[station_uic] = d

    return max_station_distance


def _determine_closest_stations(stops_data, bus_id: int, *station_uics: int) -> int:
    """
    :param stops_data: DESCRIPTION
    :type stops_data: TYPE
    :param bus_id: DESCRIPTION
    :type bus_id: int
    :param *station_uics: DESCRIPTION
    :type *station_uics: int
    :return: DESCRIPTION
    :rtype: int

    """

    distances = [(x, calculate_distance(bus_id, x, stops_data))
                  for x in station_uics]

    return min(distances, key=lambda x: x[1])[0]


# =============================================================================
# make sure station value is the parent station
# =============================================================================
def _validate_single_station(bus_map, stops_data):
    """

    :param bus_map: DESCRIPTION
    :type bus_map: TYPE
    :param stops_data: DESCRIPTION
    :type stops_data: TYPE
    :return: DESCRIPTION
    :rtype: TYPE

    """

    metro_stops = set(METRO_MAPS).union(set(METRO_MAPS_REV))
    bm = copy.deepcopy(bus_map)
    good = {}
    for k, v in bm.items():
        if len(v) == 1:
            good[k] = v.pop()
        elif all(x in metro_stops for x in v):
            new = set(METRO_MAPS.get(x, x) for x in v)
            if len(new) == 1:
                good[k] = new.pop()
            else:
                log.warning(f"Check bus stop map for {k}")
        else:
            good[k] = _determine_closest_stations(stops_data, k, *v)

    return good

def _bus_to_station_map(matches, close_stops):
    """

    :param matches: DESCRIPTION
    :type matches: TYPE
    :param close_stops: DESCRIPTION
    :type close_stops: TYPE
    :return: DESCRIPTION
    :rtype: TYPE

    """

    out = {}
    for k, v in matches.items():

        for stop_id in v:
            if not stop_id in out:
                out[stop_id] = set()

            out[stop_id].add(k)

    for k, v in close_stops.items():
        for stop_id in v.keys():
            if not stop_id in out:
                out[stop_id] = set()

            out[stop_id].add(k)

    return out


def _test_busmaps_against_previous(new_bus_maps):
    """

    :param new_bus_maps: DESCRIPTION
    :type new_bus_maps: TYPE
    :return: DESCRIPTION
    :rtype: TYPE

    """

    try:
        old_bus_maps = load_bus_maps()
    except FileNotFoundError:
        return

    for bus_id, rail_id in new_bus_maps.items():
        try:
            old_rail_id = old_bus_maps[bus_id]
        except KeyError:
            log.info(f"new_bus_id {bus_id} mapped to {rail_id}")

        if not rail_id == old_rail_id:
            log.info(f"{bus_id} changed from {old_rail_id} to {rail_id}")

    return


def bus_mapping():
    """

    :return: DESCRIPTION
    :rtype: TYPE

    """

    stops_archive = read_archive_stops()

    rail_stops = {
        k: v for k, v in stops_archive.items() if 7400000 < k < 8609999
        }
    bus_stops = {
        k: v for k, v in stops_archive.items() if k not in rail_stops
        }
    bus_frame = pd.DataFrame.from_dict(bus_stops, orient='index')

    name_matches = get_matches(rail_stops, bus_frame)

    name_matches_max_distance = \
        _calculate_match_distances(name_matches, stops_archive)

    close_stops_api = _all_nearby_stops(name_matches_max_distance)

    bmap = _bus_to_station_map(name_matches, close_stops_api)
    output =  _validate_single_station(bmap, stops_archive)

    _test_busmaps_against_previous(output)

    fp = Path(THIS_DIR, 'bus_to_station_maps.json')
    with open(fp, 'w') as f:
        json.dump(output, f)


# if __name__ == "__main__":
#     from datetime import datetime
#     st = datetime.now()
#     bus_mapping()
#     print(datetime.now() - st)





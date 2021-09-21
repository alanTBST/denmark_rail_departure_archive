# -*- coding: utf-8 -*-
"""
Created on Tue Dec  1 12:44:24 2020

@author: alkj
"""

import logging
from pathlib import Path
import warnings


import lmdb

from tools import find_date_range
from getgtfsdata import unzip_gtfs
from agency import check_agency
from busstops import bus_mapping
from calendars import check_calendars
from routes import check_routes
from shapes import check_shapes
from stops import check_stops
from stoptimes import check_stop_times
from trips import check_trips
from transfers import check_transfers
from validate import validate_files
from departures import calculate_departures
from dwh import write_to_dwh


warnings.filterwarnings("ignore")

THIS_DIR = Path(__file__).parent
LOG_DIR = Path(THIS_DIR, 'logs')

if not LOG_DIR.is_dir():
    LOG_DIR.mkdir(parents=True)

logging.basicConfig(
    filename=Path(LOG_DIR, 'log.log'),
    filemode='a',
    format='%(asctime)s; %(name)s; %(levelname)s; %(message)s',
    datefmt="%Y-%m-%d %H:%M",
    level=logging.DEBUG
    )

def main():
    """

    :return: DESCRIPTION
    :rtype: TYPE

    """


    unzip_gtfs()
    validate_files()

    check_agency()
    check_routes()
    check_stops()

    # TODO. This will just ignore this write because the memory map is full
    # from previously writing the exact same time period
    # change in future to check for duplicates and update if changes
    try:
        check_stop_times()
        check_trips()
    except lmdb.MapFullError:
        pass

    check_transfers()
    check_calendars()
    check_shapes()

    bus_mapping() # update the bus maps

    dates = find_date_range()
    rail_departures, bus_departures = calculate_departures(dates)

    write_to_dwh(bus_departures,
                 table_name='bus_departures',
                 departure_type='bus')

    write_to_dwh(rail_departures,
                 table_name='departures',
                 departure_type='rail')


if __name__ == "__main__":
    from datetime import datetime
    st = datetime.now()
    main()
    print(datetime.now() - st)

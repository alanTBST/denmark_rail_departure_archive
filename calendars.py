# -*- coding: utf-8 -*-
"""
Created on Thu Dec  3 18:54:30 2020

@author: alkj
"""

import json
from itertools import groupby
from operator import itemgetter
from pathlib import Path
import logging
from typing import Dict, Tuple, Optional

import pandas as pd

from tools import find_date_range


log = logging.getLogger(__name__)

THIS_DIR = Path(__file__).parent
ARCHIVE_DIR = Path(THIS_DIR, 'archive')
TEMP_DIR = Path(THIS_DIR, 'temp_data')

def read_calendar(filepath: Path) -> Dict[int, Dict[str, int]]:
    """
    
    :param filepath: DESCRIPTION
    :type filepath: Path
    :return: DESCRIPTION
    :rtype: Dict[int, Dict[str, int]]

    """

    df = pd.read_csv(
        filepath,
        low_memory=False,
        usecols=['service_id', 'monday',
                  'tuesday', 'wednesday',
                  'thursday', 'friday',
                  'saturday', 'sunday']).fillna(0)

    return df.set_index('service_id').T.to_dict()

def read_calendar_dates(filepath: Path) -> Dict[int, Tuple[Tuple[int, int], ...]]:
    """
    
    :param filepath: DESCRIPTION
    :type filepath: Path
    :return: DESCRIPTION
    :rtype: TYPE

    """
    
    df = pd.read_csv(filepath)
    df = df.sort_values('service_id')
    calendar_tuples = zip(df['service_id'],
                          df['date'],
                          df['exception_type'])

    return {
        key: tuple((x[1], x[2]) for x in grp) for key, grp in
        groupby(calendar_tuples, key=itemgetter(0))
        }

def write_calendar_to_archive(calendar_dict: Dict[int, Dict[str, int]]) -> None:
    """
    
    :param calendar_dict: DESCRIPTION
    :type calendar_dict: Dict[int, Dict[str, int]]
    :return: DESCRIPTION
    :rtype: None

    """
    
    dates = find_date_range(TEMP_DIR)
    fp = Path(ARCHIVE_DIR, dates, 'calendar.json')

    with open(fp, 'w') as f:
        json.dump(calendar_dict, f, indent=4)
    log.info(f"calendar written to archive in: {dates}")

def write_exceptions_to_archive(
        exception_dict: Dict[int, Tuple[Tuple[int, int], ...]]
        ) -> None:
    """
    
    :param exception_dict: DESCRIPTION
    :type exception_dict: Dict[int, Tuple[Tuple[int, int], ...]]
    :return: DESCRIPTION
    :rtype: None

    """

    dates = find_date_range(TEMP_DIR)
    fp = Path(ARCHIVE_DIR, dates, 'calendar_dates.json')

    with open(fp, 'w') as f:
        json.dump(exception_dict, f, indent=4)
    log.info(f"calendar exceptions written to archive in: {dates}")

def check_calendars() -> None:
    """
    
    :return: DESCRIPTION
    :rtype: None

    """

    new_calendar = Path(TEMP_DIR, 'calendar.txt')
    new_calendar_dates = Path(TEMP_DIR, 'calendar_dates.txt')

    calendar = read_calendar(new_calendar)
    write_calendar_to_archive(calendar)

    calendar_dates = read_calendar_dates(new_calendar_dates)
    write_exceptions_to_archive(calendar_dates)

    return


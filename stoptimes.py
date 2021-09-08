# -*- coding: utf-8 -*-
"""
Created on Thu Dec  3 09:28:42 2020

@author: alkj
"""

from pathlib import Path
import logging
from typing import Dict, Union

import pandas as pd
import lmdb
import msgpack

from tools import find_date_range

log = logging.getLogger(__name__)

THIS_DIR = Path(__file__).parent
ARCHIVE_DIR = Path(THIS_DIR, 'archive')
TEMP_DIR = Path(THIS_DIR, 'temp_data')

T_STOPS_TIMES = Dict[int, Dict[int, Dict[str, Union[str, int, float]]]]

DB_SIZE = 1 * 1024 * 1024 * 1024

def read_stop_times(filepath: Path)-> T_STOPS_TIMES:

    df = pd.read_csv(filepath, low_memory=False)
    df = df.fillna('0')
    if not 'int' in df.loc[:, 'stop_id'].dtype.name:
        df.loc[:, 'stop_id'] = \
            df.loc[:, 'stop_id'].astype(str).str.strip('G').astype(int)

    df = df.sort_values(['trip_id', 'stop_sequence'])
    df = df.set_index(['trip_id', 'stop_sequence'])

    return {
        level: df.xs(level).to_dict('index') for
        level in df.index.levels[0]
        }

def write_stops_times_to_archive(
        stop_times: T_STOPS_TIMES,
        dates: str
        ) -> None:

    archive_loc = str(Path(ARCHIVE_DIR, dates))

    with lmdb.open(archive_loc, map_size=DB_SIZE, max_dbs=5) as env:
        child_db = env.open_db(bytes('stop_times', 'utf-8'))
        with env.begin(db=child_db, write=True) as txn:
            for k, v in stop_times.items():
                key = bytes(str(k), 'utf-8')
                value = msgpack.packb(v)
                txn.put(key, value)

    log.info(f"Stoptimes data written to archive in : {dates}")


def check_stop_times() -> None:

    new_data = Path(TEMP_DIR, 'stop_times.txt')
    new_dates = find_date_range(TEMP_DIR)
    new_stop_times = read_stop_times(new_data)
    write_stops_times_to_archive(new_stop_times, new_dates)

    return
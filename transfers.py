# -*- coding: utf-8 -*-
"""
Created on Fri Dec  4 11:29:25 2020

@author: alkj
"""

import ast
import json
import logging
from pathlib import Path
from typing import Dict, Union, Tuple


import pandas as pd

log = logging.getLogger(__name__)

THIS_DIR = Path(__file__).parent
ARCHIVE_DIR = Path(THIS_DIR, 'archive')
TEMP_DIR = Path(THIS_DIR, 'temp_data')


T_TRANSFER = Dict[Tuple[int, int], Dict[str, Union[str, int]]]

def read_transfers(filepath: Path) -> T_TRANSFER:

    df = pd.read_csv(
        filepath,
        usecols=['transfer_type', 'min_transfer_time',
                  'from_stop_id', 'to_stop_id',
                  'from_route_id', 'to_route_id']
        )
    df = df.set_index(['from_stop_id', 'to_stop_id'])
    cols = df.columns

    null_columns = []
    for col in cols:
        if df.loc[:, col].isnull().all():
            null_columns.append(col)

    new_cols = [x for x in df.columns if x not in null_columns]
    df = df.loc[:, new_cols]

    for col in df.columns:
        if col in ('transfer_type', 'min_transfer_time'):
            df.loc[:, col] = df.loc[:, col].fillna(0).astype(int)
        else:
            df.loc[:, col] = df.loc[:, col].fillna('')

    return df.T.to_dict()


def read_archive_transfers() -> T_TRANSFER:

    archive_data = Path(ARCHIVE_DIR, 'transfers.json')
    with open(archive_data, 'r') as f:
        transfer_dict = json.load(f)
    return {ast.literal_eval(k): v for k, v in transfer_dict.items()}


def _check_diff(d1, d2):

    change = {k: v for k, v in d1.items() if d2[k] != v}

    return tuple((k, v, d2[k]) for k, v in change.items())

def diff_check(archive, new):

    diff = set(archive) ^ set(new)
    intersection = set(archive) & set(new)

    for transfer in diff:
        if transfer in archive:
            log.info(f"Transfer removed {transfer}")
        else:
            log.info(f"Transfer added {transfer}")

    for transfer in intersection:
        arch_transfer = archive[transfer]
        new_transfer = new[transfer]
        if not arch_transfer == new_transfer:
            changes = _check_diff(arch_transfer, new_transfer)
            for x in changes:
                log.info(
                    f"Transfers change: {transfer}: {x[0]} from {x[1]} to {x[2]}"
                    )



def write_transfers_to_archive(transfer_dict: T_TRANSFER) -> None:

    archive_loc = Path(ARCHIVE_DIR, 'transfers.json')
    transfer_dict = {str(k): v for k, v in transfer_dict.items()}

    with open(archive_loc, 'w') as f:
        json.dump(transfer_dict, f, indent=4)


def check_transfers():

    base_data = Path(THIS_DIR, 'base_data2', 'transfers.txt')
    archive_data = Path(ARCHIVE_DIR, 'transfers.json')
    new_data = Path(TEMP_DIR, 'transfers.txt')

    if not archive_data.is_file():
        try:
            base_transfers = read_transfers(base_data)
            write_transfers_to_archive(base_transfers)
        except FileNotFoundError:
            pass

    archive_transfers = read_archive_transfers()
    new_transfers = read_transfers(new_data)

    diff_check(archive_transfers, new_transfers)

    archive_transfers.update(new_transfers)
    write_transfers_to_archive(archive_transfers)

    return

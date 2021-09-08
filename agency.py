# -*- coding: utf-8 -*-
"""
Created on Tue Dec  1 14:47:38 2020

@author: alkj
"""

import os
from pathlib import Path
import json
import logging
from typing import Dict, Union

import pandas as pd

log = logging.getLogger(__name__)


T_AGENCY =  Dict[int, Dict[str, Union[str, int]]]


THIS_DIR = Path(__file__).parent
ARCHIVE_DIR = Path(THIS_DIR, 'archive')
TEMP_DIR = Path(THIS_DIR, 'temp_data')


def read_agency(filepath: Path) -> T_AGENCY:

    df = pd.read_csv(filepath).fillna('')
    df_dict = df.T.to_dict()
    d = {v['agency_id']: v for k, v in df_dict.items()}

    return d

def write_agency_to_archive(
        agency_dict: T_AGENCY
        ) -> None:
    archive_loc = Path(ARCHIVE_DIR, 'agency.json')

    with open(archive_loc, 'w', encoding='iso-8859-1') as f:
        json.dump(agency_dict, f, indent=4)
    log.info("Agency data written to archive")


def read_archive_agency() -> T_AGENCY:

    archive_loc = Path(ARCHIVE_DIR, 'agency.json')
    with open(archive_loc, 'r') as f:
        agency_archive = json.load(f)

    return {int(k): v for k, v in agency_archive.items()}


def find_dict_differences(archive: T_AGENCY, new: T_AGENCY) -> None:

    agency_id_difference = set(archive) ^ set(new)

    for agency_id in agency_id_difference:
        if agency_id not in archive:
            log.info(f"New agency added {new[agency_id]}")
        else:
            log.info(f"Agency removed  {archive[agency_id]}")


def check_agency() -> None:

    base_data = Path(THIS_DIR, 'base_data2', 'agency.txt')
    archive_data = Path(ARCHIVE_DIR, 'agency.json')
    new_data = Path(TEMP_DIR, 'agency.txt')

    if not os.path.isfile(archive_data):
        agency = read_agency(base_data)
        write_agency_to_archive(agency)

    archive_agency = read_archive_agency()
    new_agency = read_agency(new_data)
    find_dict_differences(archive_agency, new_agency)

    archive_agency.update(new_agency)
    write_agency_to_archive(archive_agency)
    return
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  2 14:24:08 2020

@author: alkj
"""

import os
from pathlib import Path
import json
import logging
from typing import Dict, Union

import pandas as pd

log = logging.getLogger(__name__)


T_ROUTE = Dict[str, Dict[str, Union[str, int]]]

THIS_DIR = Path(__file__).parent
ARCHIVE_DIR = Path(THIS_DIR, 'archive')
TEMP_DIR = Path(THIS_DIR, 'temp_data')

def read_routes(filepath: Path) -> T_ROUTE:

    df = pd.read_csv(filepath).fillna('')

    return df.set_index('route_id').T.to_dict()

def write_routes_to_archive(
        route_dict: T_ROUTE
        ) -> None:
    archive_loc = Path(ARCHIVE_DIR, 'routes.json')

    with open(archive_loc, 'w', encoding='iso-8859-1') as f:
        json.dump(route_dict, f, indent=4)
    log.info("Routes data written to archive")


def read_archive_routes() -> T_ROUTE:

    archive_loc = Path(ARCHIVE_DIR, 'routes.json')
    with open(archive_loc, 'r') as f:
        routes_archive = json.load(f)

    return routes_archive


def find_dict_differences(archive: T_ROUTE, new: T_ROUTE) -> None:

    route_id_difference = set(archive) ^ set(new)
    new = {k: {k1:v1 for k1, v1 in v.items() if v != ''} for k, v in new.items()}
    archive = {k: {k1:v1 for k1, v1 in v.items() if v != ''} for k, v in archive.items()}

    for route_id in route_id_difference:
        if route_id not in archive:
            log.info(f"New Route added {new[route_id]}")
        else:
            log.info(f"Route removed  {archive[route_id]}")

def check_routes():

    base_data = Path(THIS_DIR, 'base_data2', 'routes.txt')
    archive_data = Path(ARCHIVE_DIR, 'routes.json')
    new_data = Path(TEMP_DIR, 'routes.txt')

    if not os.path.isfile(archive_data):
        routes = read_routes(base_data)
        write_routes_to_archive(routes)

    archive_routes = read_archive_routes()
    new_routes = read_routes(new_data)
    find_dict_differences(archive_routes, new_routes)

    archive_routes.update(new_routes)
    write_routes_to_archive(archive_routes)


    return

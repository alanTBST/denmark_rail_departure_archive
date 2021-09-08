# -*- coding: utf-8 -*-
"""
Created on Mon Dec  7 10:48:57 2020

@author: alkj
"""
#Run this from within a directory containing the GTFS csv files.


import logging

from itertools import groupby
from operator import itemgetter
from pathlib import Path
from typing import Dict, Any, List, Union


import geojson
import pandas as pd
from shapely import geometry, wkt


from tools import find_date_range

log = logging.getLogger(__name__)

THIS_DIR = Path(__file__).parent
ARCHIVE_DIR = Path(THIS_DIR, 'archive')
TEMP_DIR = Path(THIS_DIR, 'temp_data')

PROP_TYPE = Dict[str, int]
GEOM_TYPE = Dict[str, Any]
FEATURE_TYPE = Dict[str, Union[str, GEOM_TYPE, PROP_TYPE]]
FEAT_COL_TYPE = Dict[str, Union[str, List[FEATURE_TYPE]]]

def read_shapes(filepath: Path) -> pd.core.frame.DataFrame:

    return pd.read_csv(filepath)

def process_shapes(shapes: pd.core.frame.DataFrame) -> FEAT_COL_TYPE:

    shapes = shapes.sort_values(['shape_id', 'shape_pt_sequence'])
    shapes = shapes.itertuples(name=None, index=False)

    shape_points = {
        key: str(tuple(str(x[2]) + ' ' + str(x[1]) for x in grp)) for
        key, grp in groupby(shapes, key=itemgetter(0))
        }

    shape_points = {k: v.replace("'", "") for
                    k, v in shape_points.items()}

    shape_points = {k: 'LINESTRING ' + v for
                    k, v in shape_points.items()}

    shape_lines = {k: wkt.loads(v) for
                   k, v in shape_points.items()}

    for_geojson = {
        "type": "FeatureCollection",
        "features": []
        }

    for k, v in shape_lines.items():
        geom = geometry.mapping(v)

        feature = {
            "type": "Feature",
            "geometry": geom,
            "properties": {
                "shape_id": k
                }
            }
        for_geojson['features'].append(feature)

    return for_geojson


def write_shapes_to_archive(shape_dict: FEAT_COL_TYPE) -> None:

    dates = find_date_range()
    fp = Path(ARCHIVE_DIR, dates, 'shapes.geojson')
    with open(fp, 'w') as f:
        geojson.dump(shape_dict, f)
    log.info(f"Shapes written to {dates}")


def check_shapes() -> None:

    new_shapes = Path(TEMP_DIR, 'shapes.txt')
    new_shapes = read_shapes(new_shapes)
    new_shapes = process_shapes(new_shapes)
    write_shapes_to_archive(new_shapes)

    return


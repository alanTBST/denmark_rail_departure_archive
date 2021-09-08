# -*- coding: utf-8 -*-
"""
Created on Wed Jun 20 11:28:53 2018

@author: alkj
"""

import logging
import os
import sys
import zipfile
from io import BytesIO
from pathlib import Path
from urllib.request import urlopen

from tools import load_config


THIS_DIR = Path(__file__).parent
TEMP_DIR = Path(THIS_DIR, 'temp_data')

log = logging.getLogger(__name__)


def unzip_gtfs() -> None:
    """
    download and extract gtfs zip
    """

    gtfs_url = load_config()['gtfs_url']
    resp = urlopen(gtfs_url)

    code = resp.code
    if code == 200:
        log.info("GTFS response success")
    else:
        log.critical("GTFS download failed - system exit")
        sys.exit(1)

    with zipfile.ZipFile(BytesIO(resp.read())) as zfile:
        for x in zfile.namelist():
            zfile.extract(x, path=TEMP_DIR)

    path = Path(TEMP_DIR, 'GTFS.zip')
    if path.exists():
        os.remove(path)

    return


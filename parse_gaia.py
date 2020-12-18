# -*- coding: utf-8 -*-
#
#  This file is part of LDTaddCAT.
#
#   This Source Code Form is subject to the terms of the Mozilla Public
#   License, v. 2.0. If a copy of the MPL was not distributed with this
#   file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Created on 18-Dec-2020
#
#  @author: tbowers

"""LDTaddCAT contains routines for importing new pointing catalogs into CAT

Lowell Discovery Telescope (Lowell Observatory: Flagstaff, AZ)
http://www.lowell.edu

The CAT (pointing catalog) tool reads catalogs from an SQL database.
This package works to process external catalogs into a form acceptable for
SQL import, and the necessary SQL interfaces.

parse_gaia.py (this file) contains routines to download and parse the Gaia
catalog(s) into a form acceptable to CAT's SQL database, and trim the catalog(s)
appropriately (in sky coverage and magnitude).
"""

from bs4 import BeautifulSoup
import requests
import wget
import numpy as np
import os

edr3_url = 'http://cdn.gea.esac.esa.int/Gaia/gedr3/gaia_source/'
fn_ext = 'gz'


def list_fd(url, ext=''):
    page = requests.get(url).text
    # print(page)
    soup = BeautifulSoup(page, 'html.parser')
    return [url + '/' + node.get('href') for node in soup.find_all('a') if
            node.get('href').endswith(ext)]


files = list_fd(edr3_url, fn_ext)

print(f'Number of files in the Gaia EDR3 catalog: {len(files)}')

for i, file in enumerate(files, 1):

    if i > 1:
        break

    lfn = file[file.rfind('/') + 1:]
    print(f'\nDownloading catalog file ({i} of {len(files)}): {lfn}')
    wget.download(file, out=lfn)

    # Numpy magic
    print(f'\nDecompressing and reading in file {lfn} ...')
    data = np.genfromtxt(lfn, delimiter=',', names=True, filling_values=np.nan)

    print(data.dtype.names)

    # Do all the interesting stuff here...

    # Remove the file from disk before cycling to the next one.
    os.remove(lfn)

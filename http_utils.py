# -*- coding: utf-8 -*-
#
#  This file is part of LOCAT.
#
#   This Source Code Form is subject to the terms of the Mozilla Public
#   License, v. 2.0. If a copy of the MPL was not distributed with this
#   file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Created on 23-Aug-2021
#
#  @author: tbowers

"""LOCAT contains routines for importing new pointing catalogs into CAT

Lowell Discovery Telescope (Lowell Observatory: Flagstaff, AZ)
http://www.lowell.edu

The CAT (pointing catalog) tool reads catalogs from an SQL database.
This package works to process external catalogs into a form acceptable for
SQL import, and the necessary SQL interfaces.

http_utils.py (this file) contains routines to download and parse files from
any online archive.
"""

# Built-In Libraries
import os
import requests
import time

# Third-Party Libraries
from bs4 import BeautifulSoup
from tqdm import tqdm


def list_http_directory(url, ext=''):
    """list_http_directory Returns the list of HTTP files matching `ext`

    Given a remote HTTP directory, returns the list of files matching `ext`

    Parameters
    ----------
    url : `str`
        The URL of the directory for which a file list is desired
    ext : `str`, optional
        File extensions to be considered.  [Default: '']

    Returns
    -------
    `list`
        List of files linked at URL (full URL for each file)
    """
    html_page = requests.get(url).text
    soup = BeautifulSoup(html_page, 'html.parser')
    return [url + '/' + link.get('href') for link in soup.find_all('a') if
            link.get('href').endswith(ext)]


def download_file(file, lfn, throttle):
    # Streaming, so we can iterate over the http_respond
    http_respond = requests.get(file, stream=True, timeout=10)
    file_size_bytes = int(http_respond.headers.get('content-length', 0))
    progress_bar = tqdm(total=file_size_bytes, unit='B',
                        unit_scale=True)

    while progress_bar.n != file_size_bytes:
        with open(tempfn := f'{lfn}.tmp', 'wb') as f:
            try:
                # Update the progress bar for each 100kB downloaded
                for data in http_respond.iter_content(1024 * 100):
                    progress_bar.update(len(data))
                    f.write(data)
                    if throttle:
                        # Sleeping for 0.2 seconds means a continuous
                        #  download speed of ~0.5 MB/s
                        time.sleep(0.2)
            except requests.ConnectionError:
                errmsg = f'Connection Error occurred.'
            except requests.ReadTimeout:
                errmsg = f'Read Timeout error occurred.'
            else:
                errmsg = f'Unspecified error occurred.'

        if file_size_bytes != 0 and progress_bar.n != file_size_bytes:
            print(f'{errmsg}  Trying again...')
            # Reload the requests.get() object (go back up the creek)
            http_respond = requests.get(file, stream=True, timeout=10)
            # Reset the progress bar object to restart
            progress_bar.reset()
        else:
            # Close the progress bar, and move the temp file to lfn
            progress_bar.close()
            os.rename(tempfn, lfn)



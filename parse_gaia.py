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

# Built-In Libraries
import os
import requests

# Third-Party Libraries
from bs4 import BeautifulSoup
import wget

# Numpy
import numpy as np

# Astropy
from astropy.table import Table


def list_http_directory(url, ext=''):
    """Given a remote HTTP directory, returns the list of files matching ext

    :param url: The URL of the directory for which a file list is desired
    :param ext: File extensions to be considered.  Default=''
    :return: List of files linked at URL
    """
    html_page = requests.get(url).text
    soup = BeautifulSoup(html_page, 'html.parser')
    return [url + '/' + link.get('href') for link in soup.find_all('a') if
            link.get('href').endswith(ext)]


def parse_edr3(test_one=True, use_existing=True):
    """Parse each catalog file of Gaia EDR3 into a binary FITS table
    Each EDR3 catalog file contains oodles of information about each source,
    but for the purposes of the CAT, we only need a small subset of this
    information.  Furthermore, we don't need to include in our version of the
    Gaia EDR3 catalog objects we either can't see from LDT (too negative
    declination) or are too faint to use for guiding (Vmag > 18).  The resulting
    FITS tables are ~3% the size of each of the original (compressed) CSV files.

    The output FITS tables have the same name as the original EDR3 files, but
    with FITS extension.
    
    :param use_existing: `bool` Check for and use existing EDR3 file
    :param test_one: `bool` Just run on one file, for testing
    :return: None.
    """
    # Gaia EDR3 photometry conversions, G - V = f(G_BP - G_RP)
    # From Riello, et al. 2020.  A&A, vvv, nnn, Table C2
    a = [-0.02704, 0.01424, -0.2156, 0.01426]

    # Gaia EDR3 catalog location
    edr3_url = 'http://cdn.gea.esac.esa.int/Gaia/gedr3/gaia_source/'

    # Get a list of the catalog files at this location.  All CSV files are .gz
    files = list_http_directory(edr3_url, 'gz')

    # Start working our way through the list
    print(f'Number of files in the Gaia EDR3 catalog: {len(files)}')
    for i, file in enumerate(files, 1):

        # Testing condition
        if test_one and i > 1:
            break

        # Construct the local filename.  Really, just the remote w/o the path
        lfn = file[file.rfind('/') + 1:]
        fitsfn = f'{lfn[:lfn.find(".")]}.fits'

        # Check if related FITS table exists.  If yes, skip to the next one.
        if os.path.isfile(fitsfn):
            continue

        # To prevent multiple copies of a .csv.gz file, remove or use existing
        if os.path.isfile(lfn) and not use_existing:
            os.remove(lfn)

        # Download the appropriate EDR3 catalog file
        if not os.path.isfile(lfn):
            print(f'\nDownloading catalog file ({i} of {len(files)}): {lfn}')
            wget.download(file, out=lfn)

        # Use Numpy's genfromtxt function, which will also decompress!
        print(f'\nDecompressing and reading in file {lfn} ...')
        data = np.genfromtxt(lfn, delimiter=',', names=True,
                             filling_values=np.nan)

        # Use the Gaia EDR3 photometry conversion to yield Johnson V magnitude
        vmag = data["phot_g_mean_mag"] - a[0] - a[1] * data["bp_rp"] - \
               a[2] * data["bp_rp"] ** 2 - a[3] * data["bp_rp"] ** 3

        # Next, work to cull out the objects that do not meet our requirements
        print(f'N sources in this file: {len(data["ra"])}')

        # Cull out objects too dim
        data = data[(magind := np.where(vmag <= 18))]
        vmag = vmag[magind]
        print(f'Sources after mag limit: {len(data["ra"])}')

        # Cull out objects too far south
        data = data[np.where(data["dec"] > -40)]
        print(f'Sources after dec limit: {len(data["ra"])}')

        # Print some summary statistics to the screen
        print(f'R.A. range: {np.min(data["ra"]):.4f} - ' +
              f'{np.max(data["ra"]):.4f}')
        print(f'Dec. range: {np.min(data["dec"]):.4f} - ' +
              f'{np.max(data["dec"]):.4f}')
        print(f'Vmag range: {np.nanmin(vmag):.2f} - ' +
              f'{np.nanmax(vmag):.2f}')

        # Construct the parts needed for the CAT
        # Name = 'source_id'                       (unsigned 64-bit integer)
        name = data['source_id'].astype('u8')
        # Right Ascension = 'ra'                   (64-bit float)
        ra = data['ra'].astype('f8')
        # Declination = 'dec'                      (64-bit float)
        dec = data['dec'].astype('f8')
        # Epoch = 'ref_epoch'                      (32-bit float)
        epoch = data['ref_epoch'].astype('f4')
        # Proper Motion in RA * cos(dec)= 'pmra'   (32-bit float)
        pmra = data['pmra'].astype('f4')
        # Proper Motion in Declincation = 'pmdec'  (32-bit float)
        pmdec = data['pmdec'].astype('f4')
        # Magnitude = 'vmag'                       (32-bit float)
        vmag = vmag.astype('f4')
        # G-band magnitude = 'phot_g_mean_mag'     (32-bit float)
        g_mag = data['phot_g_mean_mag'].astype('f4')
        # G_BP magnitude = 'phot_bp_mean_mag'      (32-bit float)
        g_bp = data['phot_bp_mean_mag'].astype('f4')
        # G_RP magnitude = 'phot_rp_mean_mag'      (32-bit float)
        g_rp = data['phot_rp_mean_mag'].astype('f4')

        # Stuff all of these into an AstroPy table, which has easy writing
        t = Table([name, ra, dec, epoch, pmra, pmdec, vmag, g_mag, g_bp, g_rp],
                  names=['name', 'ra', 'dec', 'epoch', 'pmra', 'pmdec', 'vmag',
                         'g_mag', 'g_bp', 'g_rp'])

        # Print out the first line of the table for a sanity check
        print(t[0])

        # Write the table to a FITS binary table, using the same filename
        t.write(fitsfn, overwrite=True)

        # Remove the .csv.gz file from disk before cycling to the next one.
        if not use_existing:
            os.remove(lfn)


def main():
    """
    This is the main body function.
    """
    pass


if __name__ == "__main__":
    main()

# -*- coding: utf-8 -*-
#
#  This file is part of LOCAT.
#
#   This Source Code Form is subject to the terms of the Mozilla Public
#   License, v. 2.0. If a copy of the MPL was not distributed with this
#   file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Created: 18-Dec-2020
#  Modified: 23-Aug-2021
#
#  @author: tbowers

"""LOCAT contains routines for importing new pointing catalogs into CAT

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
import glob
import os

# Third-Party Libraries
import numpy as np
from astropy.io.registry import IORegistryError
from astropy.table import Table, vstack
from tqdm import tqdm

# General CAT routines
from .http_utils import *


def parse_edr3(test_one=True, use_existing=True, throttle=None):
    """parse_edr3 Parse each catalog file of Gaia EDR3 into a binary FITS table

    Each EDR3 catalog file contains oodles of information about each source,
    but for the purposes of the CAT, we only need a small subset of this
    information.  Furthermore, we don't need to include in our version of the
    Gaia EDR3 catalog objects we either can't see from LDT (too negative
    declination) or are too faint to use for guiding (Vmag > 18).  The resulting
    FITS tables are ~3% the size of each of the original (compressed) CSV files.

    The output FITS tables have the same name as the original EDR3 files, but
    with FITS extension.


    Parameters
    ----------
    test_one : `bool`, optional
        Only run on one EDR3 file for testing [Default: True]
    use_existing : `bool`, optional
        Check for and use existing EDR3 file [Default: True]
    throttle : `float`, optional
        Limit bandwidth usage (MB/s) [Default: None]
    """

    # Gaia EDR3 catalog location
    edr3_url = 'http://cdn.gea.esac.esa.int/Gaia/gedr3/gaia_source/'

    # Get a list of the catalog files at this location.  All CSV files are .gz
    files = list_http_directory(edr3_url, 'gz')

    # Start working our way through the list
    print(f"Number of files in the Gaia EDR3 catalog: {len(files)}")
    for i, file in enumerate(files, 1):

        # Testing condition
        if test_one and i > 1:
            break

        # Construct the local filename.  Really, just the remote w/o the path
        lfn = file.split('/')[-1]
        fitsfn = f"{lfn.split('.')[0]}.fits"

        # Check if related FITS table exists.  If yes, skip to the next one.
        if os.path.isfile(fitsfn):
            continue

        # To prevent multiple copies of a .csv.gz file, remove or use existing
        if os.path.isfile(lfn) and not use_existing:
            os.remove(lfn)

        # Download the appropriate EDR3 catalog file
        while not os.path.isfile(lfn):
            print(f"\nDownloading catalog file ({i} of {len(files)}, " + \
                  f"{i / len(files) * 100:.0f}%): {lfn}")
            download_file(file, lfn, throttle)

        # Read into AstroPy table, which will also decompress!
        print(f"\nDecompressing and reading in file {lfn} ...")
        data = Table.read(lfn, format='ascii.csv')

        # Use the Gaia EDR3 photometry conversion to yield Johnson V magnitude
        V, R, I = convert_edr3_phot_jc(data['phot_g_mean_mag'], data['bp_rp'])
        data['vmag'] = V
        data['V'] = V
        data['R'] = R
        data['I'] = I

        # Next, work to cull out the objects that do not meet our requirements
        print(f"N sources in this file: {len(data['ra'])}")

        # Save declinations for sanity check
        gaia_dec = data['dec'].copy()

        # Cull out objects too dim
        data = data[np.where(data['vmag'] <= 18)]
        print(f"Sources after mag limit: {len(data['ra'])}")

        # Cull out objects too far south
        data = data[np.where(data['dec'] > -40)]
        print(f"Sources after dec limit: {len(data['ra'])}")

        # If entire file out-of-range, remove downloaded file & continue
        if len(data['ra']) == 0:
            print(f"Catalog declination range: {np.nanmin(gaia_dec):.4f} - " +
                  f"{np.nanmax(gaia_dec):.4f}")
            print("No sources from this file to be saved.")
            # Create empty FITS file for bookkeeping
            open(fitsfn, 'a').close()
            os.remove(lfn)
            continue

        # Print some summary statistics to the screen
        print(f"R.A. range: {np.min(data['ra']):.4f} - " +
              f"{np.max(data['ra']):.4f}")
        print(f"Dec. range: {np.min(data['dec']):.4f} - " +
              f"{np.max(data['dec']):.4f}")
        print(f"Vmag range: {np.nanmin(data['vmag']):.2f} - " +
              f"{np.nanmax(data['vmag']):.2f}")

        #======================================#
        # Construct the parts needed for the CAT
        t = Table()

        # Name = 'source_id'                       (unsigned 64-bit integer)
        t['name'] = data['source_id'].astype('u8')

        # Right Ascension = 'ra'                   (64-bit float)
        # Declination = 'dec'                      (64-bit float)
        for col in ['ra', 'dec']:
            t[col] = data[col].astype('f8')

        # Proper Motion in RA * cos(dec)= 'pmra'   (32-bit float)
        # Proper Motion in Declincation = 'pmdec'  (32-bit float)
        # Magnitude = 'vmag'                       (32-bit float)
        # Johnson-Cousins V = 'V'                  (32-bit float)
        # Johnson-Cousins R = 'R'                  (32-bit float)
        # Johnson-Cousins I = 'I'                  (32-bit float)
        for col in ['pmra', 'pmdec', 'vmag', 'V', 'R', 'I']:
            t[col] = data[col].astype('f4')

        # Epoch = 'ref_epoch'                      (32-bit float)
        # G-band magnitude = 'phot_g_mean_mag'     (32-bit float)
        # G_BP magnitude = 'phot_bp_mean_mag'      (32-bit float)
        # G_RP magnitude = 'phot_rp_mean_mag'      (32-bit float)
        for c1, c2 in zip(['epoch', 'g_mag', 'g_bp', 'g_rp'],
                          ['ref_epoch', 'phot_g_mean_mag', 'phot_bp_mean_mag',
                           'phot_rp_mean_mag']):
            t[c1] = data[c2].astype('f4')

        # Print out the first line of the table for a sanity check
        print(t[0])

        # Write the table to a FITS binary table, using the same filename
        t.write(fitsfn, overwrite=True)

        # Remove the .csv.gz file from disk before cycling to the next one.
        if not use_existing:
            os.remove(lfn)


def convert_edr3_phot_jc(g, br):
    """convert_edr3_phot_jc Convert EDR3 photometry to Johnson-Cousins VRI

    [extended_summary]

    Parameters
    ----------
    g : `array-like`
        The Gaia EDR3 G-band photometry magnitude
    br : `array-like`
        The Gaia EDR3 `B-R` color from the subband photometries

    Returns
    -------
    `tuple` of `array-like`
        V, R, I magnitudes for the input(s)
    """
    # Gaia EDR3 photometry conversions
    # From Riello, et al. 2021.  A&A, 649, A3, Table C2

    # G - V = f(a, G_BP - G_RP)
    a = [-0.02704, 0.01424, -0.2156, 0.01426]
    vmag = g - a[0] - a[1]*br - a[2]*br**2 - a[3]*br**3

    # G - R = f(b, G_BP - G_RP)
    b = [-0.02275, 0.3961, -0.1243, -0.01396, 0.003775]
    rmag = g - b[0] - b[1]*br - b[2]*br**2 - b[3]*br**3 - b[4]*br**4

    # G - I = f(c, G_BP - G_RP)
    c = [0.01753, 0.76, -0.0991]
    imag = g - c[0] - c[1]*br - c[2]*br**2

    return vmag, rmag, imag


def recompile_edr3_catalog():
    
    decband_fn = []
    # Create empty dec-band files
    for dec_min in range(-40, 90, 10):

        # Construct the filename, create and write an empty table
        fn = f"Gaia_EDR3_dec_{dec_min:+d}_{dec_min+10:+d}.fits"
        decband_fn.append(fn)
        t = Table()
        t.write(fn, overwrite=True)

    # Get list of processed EDR3 catalog files
    gaia_files = sorted(glob.glob("GaiaSource_*.fits"))

    progress_bar = tqdm(total=len(gaia_files), unit=' file',
                        unit_scale=True, )

    while progress_bar.n != len(gaia_files):
        # Read in each of the processed EDR3 files
        for gaia in gaia_files:
            try:
                t = Table.read(gaia)
            # The IORegistryError occurs when we created an empty file above
            except IORegistryError:
                continue

            # Find which dec bands are represented by this EDR3 file
            dec_range = np.array([np.min(t['dec']), np.max(t['dec'])])
            band_range = np.floor(dec_range/10.)*10
            bands = np.arange(band_range[0], band_range[1]+1, 10)

            # Open the dec-band file(s) and append the objects from this EDR3 file
            for band in bands:
                fn = f"Gaia_EDR3_dec_{band:+.0f}_{band+10:+.0f}.fits"
                dec_table = Table.read(fn)

                # Get list of objects in this band
                idx = np.where(np.logical_and(t['dec'] >= band, t['dec'] < band+10))

                # Append the objects from this dec band to the appropriate table
                dec_table = vstack([dec_table, t[idx]])
                dec_table.write(fn, overwrite=True)
            
            progress_bar.update(1)

    # Go through the dec-band files and sort each by RA before resaving.
    dec_files = sorted(glob.glob(f"Gaia_EDR3_dec_*.fits"))
    for dec_file in dec_files:
        t = Table.read(dec_file)
        t.sort('ra')
        t.write(dec_file, overwrite=True)


#======================================
def main(args=['parse_gaia.py'], throttle=None):
    """
    This is the main body function.
    """
    throttle = float(args[1]) if len(args) > 1 else None
    print("Downloasing and parding Gaia EDR3 catalog files...")
    parse_edr3(test_one=False, use_existing=False, throttle=throttle)
    print("Recompiling downloaded Gaia EDR3 files into dec bands...")
    recompile_edr3_catalog()

if __name__ == "__main__":
    import sys
    main(sys.argv)

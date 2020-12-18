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

from astroquery.gaia import Gaia

tables = Gaia.load_tables(only_names=True)
for table in tables:
    print(table.get_qualified_name())

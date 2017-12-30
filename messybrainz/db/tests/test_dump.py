""" This module contains data dump creation and import tests.
"""

# messybrainz-server - Server for the MessyBrainz project
#
# Copyright (C) 2017 MetaBrainz Foundation Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version. #
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA)

import messybrainz.db as db
import messybrainz.db.dump as db_dump
import tempfile

from messybrainz.testing import DatabaseTestCase


class DumpTestCase(DatabaseTestCase):

    def setUp(self):
        super().setUp()
        self.tempdir = tempfile.mkdtemp()


    def tearDown(self):
        super().tearDown()
        shutil.rmtree(self.tempdir)


    def test_create_dump(self):
        archive = db_dump.dump_db(self.tempdir)
        self.assertTrue(os.path.isfile(archive))



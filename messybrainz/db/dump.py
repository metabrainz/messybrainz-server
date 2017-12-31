""" This module contains data dump creation and import functions
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


import logging
import os
import shutil
import sqlalchemy
import subprocess
import sys
import tarfile
import tempfile

from datetime import datetime
from messybrainz import db
from messybrainz.db.utils import create_path


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


DUMP_LICENSE_FILE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                      'licenses', 'COPYING-PublicDomain')


TABLES = (
    'recording',
    'recording_json',
    'artist_credit',
    'release',
    'recording_redirect',
    'recording_cluster',
    'artist_credit_redirect',
    'artist_credit_cluster',
    'release_redirect',
    'release_cluster',
)


def dump_db(location, threads=None, dump_time=datetime.today()):
    """ Create MessyBrainz database dump in the specified location

        Arguments:
            location: Directory where the final dump will be stored
            threads: Maximal number of threads to run during compression, 1 if not specified
            dump_time (datetime obj): the time when the dump was initiated

        Returns:
            Path to created dump.
    """
    create_path(location)
    archive_name = 'messybrainz-data-dump-{time}'.format(
        time=dump_time.strftime('%Y%m%d-%H%M%S')
    )
    archive_path = os.path.join(location, '{archive_name}.tar.xz'.format(
        archive_name=archive_name,
    ))

    with open(archive_path, 'w') as archive:

        # construct the pxz command to compress data
        pxz_command = ['pxz', '--compress']
        if threads is not None:
            pxz_command.append('-T {threads}'.format(threads=threads))

        pxz = subprocess.Popen(pxz_command, stdin=subprocess.PIPE, stdout=archive)

        with tarfile.open(fileobj=pxz.stdin, mode='w|') as tar:

            temp_dir = tempfile.mkdtemp()

            # add metadata
            try:
                schema_seq_path = os.path.join(temp_dir, "SCHEMA_SEQUENCE")
                with open(schema_seq_path, "w") as f:
                    f.write(str(db.SCHEMA_VERSION))
                tar.add(schema_seq_path,
                        arcname=os.path.join(archive_name, "SCHEMA_SEQUENCE"))
                timestamp_path = os.path.join(temp_dir, "TIMESTAMP")
                with open(timestamp_path, "w") as f:
                    f.write(dump_time.isoformat(" "))
                tar.add(timestamp_path,
                        arcname=os.path.join(archive_name, "TIMESTAMP"))
                tar.add(DUMP_LICENSE_FILE_PATH,
                        arcname=os.path.join(archive_name, "COPYING"))
            except IOError as e:
                logger.error('IOError while adding dump metadata...')
                raise
            except Exception as e:
                logger.error('Exception while adding dump metadata: %s', str(e))
                raise

            # copy tables to archive_tables_dir and then add it to the archive
            archive_tables_dir = os.path.join(temp_dir, 'data')
            create_path(archive_tables_dir)
            with db.engine.connect() as connection:
                with connection.begin() as transaction:
                    cursor = connection.connection.cursor()
                    for table in TABLES:
                        try:
                            with open(os.path.join(archive_tables_dir, table), 'w') as f:
                                cursor.copy_to(f, '(SELECT * FROM {table})'.format(table=table))
                        except IOError as e:
                            logger.error('IOError while copying table %s', table)
                            raise
                        except Exception as e:
                            logger.error('Error while copying table %s: %s', table, str(e))
                            raise
                    transaction.rollback()

            tar.add(archive_tables_dir, arcname=os.path.join(archive_name, 'data'))
            shutil.rmtree(temp_dir)

        pxz.stdin.close()

    return archive_path


def add_dump_entry(timestamp):
    """ Adds an entry to the data_dump table with specified time.

        Args:
            timestamp: the unix timestamp to be added

        Returns:
            id (int): the id of the new entry added
    """

    with db.engine.connect() as connection:
        result = connection.execute(sqlalchemy.text("""
                INSERT INTO data_dump (created)
                     VALUES (TO_TIMESTAMP(:ts))
                  RETURNING id
            """), {
                'ts': timestamp,
            })
        return result.fetchone()['id']


def get_dump_entries():
    """ Returns a list of all dump entries in the data_dump table
    """

    with db.engine.connect() as connection:
        result = connection.execute(sqlalchemy.text("""
                SELECT id, created
                  FROM data_dump
              ORDER BY created DESC
            """))

        return [dict(row) for row in result]

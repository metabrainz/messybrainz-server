""" This module contains a click group with commands to
create and import postgres data dumps.
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


import click
import logging
import messybrainz.db.dump as db_dump
import os

from messybrainz import db

import messybrainz.default_config as config
try:
    import messybrainz.custom_config as config
except ImportError:
    pass

logger = logging.getLogger(__name__)


cli = click.Group()


@cli.command()
@click.option('--location', '-l', default=os.path.join(os.getcwd(), 'messybrainz-export'))
@click.option('--threads', '-t', type=int)
def create(location, threads=None):
    """ Create a MessyBrainz data dump

        Args:
            location (str): path to the directory where the dump should be made
            threads (int): the number of threads to be used while compression, 1 if not specified
    """
    db.init_db_engine(config.SQLALCHEMY_DATABASE_URI)
    logger.info('Beginning data dump...')
    try:
        dump_path = db_dump.dump_db(location, threads)
    except IOError as e:
        logger.error('IOError while dumping MessyBrainz database: %s', str(e))
        raise
    logger.info('Data dump created at %s!', dump_path)


if __name__ == '__main__':
    cli()

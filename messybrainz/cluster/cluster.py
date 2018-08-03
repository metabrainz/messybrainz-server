#!/usr/bin/env python3

import pika
import sys
import ujson
from brainzutils import musicbrainz_db
from brainzutils.musicbrainz_db.exceptions import NoDataFoundException
from flask import current_app
from messybrainz.cluster import utils
from messybrainz import db
from messybrainz.db import data
from messybrainz.db import artist as db_artist
from messybrainz.db import recording as db_recording
from messybrainz.db import release as db_release
from messybrainz.webserver import create_app
from sqlalchemy.exc import IntegrityError
from uuid import UUID


class Cluster:
    def __init__(self):
        self.connection = None
        self.incoming_ch = None
        self.unique_ch = None
        self.ERROR_RETRY_DELAY = 3 # number of seconds to wait until retrying an operation


    @staticmethod
    def static_callback(ch, method, properties, body, obj):
        return obj.callback(ch, method, properties, body)


    def connect_to_rabbitmq(self):
        connection_config = {
            'username': current_app.config['RABBITMQ_USERNAME'],
            'password': current_app.config['RABBITMQ_PASSWORD'],
            'host': current_app.config['RABBITMQ_HOST'],
            'port': current_app.config['RABBITMQ_PORT'],
            'virtual_host': current_app.config['RABBITMQ_VHOST'],
        }
        self.connection = utils.connect_to_rabbitmq(**connection_config,
                                                    error_logger=current_app.logger.error,
                                                    error_retry_delay=self.ERROR_RETRY_DELAY)


    def callback(self, ch, method, properties, body):
        self.cluster_recording(body)
        while True:
            try:
                self.incoming_ch.basic_ack(delivery_tag = method.delivery_tag)
                break
            except pika.exceptions.ConnectionClosed:
                self.connect_to_rabbitmq()


    def cluster_recording(self, rec):

        rec_data = ujson.loads(rec)
        if 'recording_mbid' in rec_data:
            try:
                with db.engine.begin() as connection:
                    cluster_id = db_recording.get_recording_cluster_id_using_recording_mbid(connection, rec_data['recording_mbid'])
                    msid = data.get_id_from_recording(connection, rec_data)
                    if cluster_id:
                        db_recording.insert_recording_cluster(connection, cluster_id, [msid])
                    else:
                        db_recording.insert_recording_cluster(connection, msid, [msid])
                        db_recording.link_recording_mbid_to_recording_msid(connection, msid, rec_data['recording_mbid'])

                    if 'artist_mbids' not in rec_data:
                        try:
                            artist_mbids = db_artist.fetch_artist_mbids(connection, rec_data['recording_mbid'])
                            artist_mbids = [UUID(artist_mbid) for artist_mbid in artist_mbids]
                            artist_mbids.sort()

                            cluster_id = db_artist.get_artist_cluster_id_using_artist_mbids(connection, artist_mbids)
                            msid = data.get_artist_credit(connection, rec_data['artist'])
                            if cluster_id:
                                db_artist.insert_artist_credit_cluster(connection, cluster_id, [msid])
                            else:
                                db_artist.insert_artist_credit_cluster(connection, msid, [msid])
                                db_artist.link_artist_mbids_to_artist_credit_cluster_id(connection, msid, artist_mbids)
                        except (IntegrityError, NoDataFoundException):
                            pass

                    if 'release' in rec_data and 'release_mbid' not in rec_data:
                        try:
                            releases = db_release.fetch_releases_from_musicbrainz_db(connection, rec_data['recording_mbid'])
                            for release in releases:
                                if release['name'] == rec_data['release']:
                                    cluster_id = db_release.get_release_cluster_id_using_release_mbid(connection, release['id'])
                                    msid = data.get_release(connection, rec_data['release'])
                                    if cluster_id:
                                        db_release.insert_release_cluster(connection, cluster_id, [msid])
                                    else:
                                        db_release.insert_release_cluster(connection, msid, [msid])
                                        db_release.link_release_mbid_to_release_msid(connection, msid, release['id'])
                        except (IntegrityError, NoDataFoundException):
                            pass
            except IntegrityError:
                pass

        if 'artist_mbids' in rec_data:
            artist_mbids = [UUID(artist_mbid) for artist_mbid in rec_data['artist_mbids']]
            try:
                with db.engine.begin() as connection:
                    cluster_id = db_artist.get_artist_cluster_id_using_artist_mbids(connection, artist_mbids)
                    msid = data.get_artist_credit(connection, rec_data['artist'])
                    if cluster_id:
                        db_artist.insert_artist_credit_cluster(connection, cluster_id, [msid])
                    else:
                        db_artist.insert_artist_credit_cluster(connection, msid, [msid])
                        db_artist.link_artist_mbids_to_artist_credit_cluster_id(connection, msid, artist_mbids)
            except IntegrityError:
                pass

        if 'release_mbid' in rec_data and 'release' in rec_data:
            try:
                with db.engine.begin() as connection:
                    cluster_id = db_release.get_release_cluster_id_using_release_mbid(connection, rec_data['release_mbid'])
                    msid = data.get_release(connection, rec_data['release'])
                    if cluster_id:
                        db_release.insert_release_cluster(connection, cluster_id, [msid])
                    else:
                        db_release.insert_release_cluster(connection, msid, [msid])
                        db_release.link_release_mbid_to_release_msid(connection, msid, rec_data['release_mbid'])
            except IntegrityError:
                pass

        while True:
            try:
                self.unique_ch.basic_publish(
                    exchange=current_app.config['UNIQUE_EXCHANGE'],
                    routing_key='',
                    body=rec,
                    properties=pika.BasicProperties(delivery_mode = 2,),
                )
                break
            except pika.exceptions.ConnectionClosed:
                self.connect_to_rabbitmq()
        

    def _verify_hosts_in_config(self):
        if "RABBITMQ_HOST" not in current_app.config:
            current_app.logger.critical("RabbitMQ service not defined. Sleeping {0} seconds and exiting.".format(self.ERROR_RETRY_DELAY))
            time.sleep(self.ERROR_RETRY_DELAY)
            sys.exit(-1)


    def start(self):
        app = create_app()
        with app.app_context():
            current_app.logger.info("cluster init")
            self._verify_hosts_in_config()

            while True:
                try:
                    db.init_db_engine(current_app.config['SQLALCHEMY_DATABASE_URI'])
                    break
                except Exception as err:
                    current_app.logger.error("Cannot connect to db: %s. Retrying in 2 seconds and trying again." % str(err), exc_info=True)
                    sleep(self.ERROR_RETRY_DELAY)

            while True:
                try:
                    musicbrainz_db.init_db_engine(current_app.config['MB_DATABASE_URI'])
                    break
                except Exception as err:
                    current_app.logger.error("Cannot connect to MusicBrainz db: %s. Retrying in 2 seconds and trying again." % str(err), exc_info=True)
                    sleep(self.ERROR_RETRY_DELAY)

            while True:
                self.connect_to_rabbitmq()
                self.incoming_ch = self.connection.channel()
                self.incoming_ch.exchange_declare(exchange=current_app.config['INCOMING_EXCHANGE'], exchange_type='fanout')
                self.incoming_ch.queue_declare(current_app.config['INCOMING_QUEUE'], durable=True)
                self.incoming_ch.queue_bind(exchange=current_app.config['INCOMING_EXCHANGE'], queue=current_app.config['INCOMING_QUEUE'])
                self.incoming_ch.basic_consume(
                    lambda ch, method, properties, body: self.static_callback(ch, method, properties, body, obj=self),
                    queue=current_app.config['INCOMING_QUEUE'],
                )

                self.unique_ch = self.connection.channel()
                self.unique_ch.exchange_declare(exchange=current_app.config['UNIQUE_EXCHANGE'], exchange_type='fanout')

                current_app.logger.info("Clustering started")
                try:
                    self.incoming_ch.start_consuming()
                except pika.exceptions.ConnectionClosed:
                    current_app.logger.warn("Connection to rabbitmq closed. Re-opening.", exc_info=True)
                    self.connection = None
                    continue

                self.connection.close()


if __name__ == "__main__":
    cl = Cluster()
    cl.start()

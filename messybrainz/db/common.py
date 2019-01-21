from brainzutils.musicbrainz_db import release as mb_release
from brainzutils.musicbrainz_db import artist as mb_artist
from messybrainz import db
import logging


def create_entity_clusters(create_without_anomalies, create_with_anomalies):
    """Takes two functions which create clusters for a given entity.

    Args:
        create_without_anomalies(function): this functions is responsible for creating
        clusters without considering anomalies.
        create_with_anomalies(function): this function will create clusters for the
        anomalies (A single MSID pointing to multiple MBIDs in entity_redirect table).

    Returns:
        clusters_modified (int): number of clusters modified.
        clusters_added_to_redirect (int): number of clusters added to redirect table.
    """

    clusters_modified = 0
    clusters_added_to_redirect = 0

    with db.engine.connect() as connection:
        clusters_modified, clusters_added_to_redirect = create_without_anomalies(connection)
        clusters_added_to_redirect += create_with_anomalies(connection)

    return clusters_modified, clusters_added_to_redirect


def create_entity_clusters_for_anomalies(connection,
                                        fetch_entities_left_to_cluster,
                                        get_entity_gids_from_recording_json_using_mbids,
                                        get_cluster_id_using_msid,
                                        link_entity_mbid_to_entity_cluster_id,
                                        get_recordings_metadata_using_entity_mbid):
    """Creates entity clusters for the anomalies (A single MSID pointing
       to multiple MBIDs in entity_redirect table).

    Args:
        connection: the sqlalchemy db connection to be used to execute queries
        fetch_entities_left_to_cluster(function): Returns mbids for the entity MBIDs that
                                                were not clustered after executing the
                                                first phase of clustering (clustering without
                                                considering anomalies). These are anomalies
                                                (A single MSID pointing to multiple MBIDs in
                                                entity_redirect table).
        get_entity_gids_from_recording_json_using_mbids(function): Returns entity MSIDs using
                                                                an entity MBID.
        get_cluster_id_using_msid(function): Gets the cluster ID for a given MSID.
        link_entity_mbid_to_entity_cluster_id(function): Links the entity mbid to the cluster_id.
        get_recordings_metadata_using_entity_mbid(function): gets recordings metadata using given MBID.

    Returns:
        clusters_add_to_redirect (int): number of clusters added to redirect table.
    """

    logger = logging.getLogger(__name__)
    logger_level = logger.getEffectiveLevel()

    logger.debug("Creating clusters for anomalies...")
    clusters_add_to_redirect = 0
    entities_left = fetch_entities_left_to_cluster(connection)
    for entity_mbid in entities_left:
        entity_gids = get_entity_gids_from_recording_json_using_mbids(connection, entity_mbid)
        cluster_ids = {get_cluster_id_using_msid(connection, entity_gid) for entity_gid in entity_gids}
        for cluster_id in cluster_ids:
            link_entity_mbid_to_entity_cluster_id(connection, cluster_id, entity_mbid)
            clusters_add_to_redirect += 1

            if logger_level == logging.DEBUG:
                recordings = get_recordings_metadata_using_entity_mbid(connection, entity_mbid)
                _print_debug_info(connection, logger, cluster_id, entity_gids, entity_mbid, recordings)

    logger.debug("\nClusters added to redirect table: {0}.".format(clusters_add_to_redirect))

    return clusters_add_to_redirect


def create_entity_clusters_without_considering_anomalies(connection,
                                                        fetch_unclustered_entity_mbids,
                                                        fetch_unclustered_gids_for_entity_mbids,
                                                        get_entity_cluster_id_using_entity_mbids,
                                                        link_entity_mbids_to_entity_cluster_id,
                                                        insert_entity_cluster,
                                                        get_recordings_metadata_using_entity_mbid):
    """Creates cluster for entity without considering anomalies (A single MSID pointing
       to multiple MBIDs in entity_redirect table).

    Args:
        connection: the sqlalchemy db connection to be used to execute queries.
        fetch_unclustered_entity_mbids (function): Fetch all the distinct entity
                                                MBIDs we have in recording_json table
                                                but don't have their corresponding MSIDs
                                                in entity_cluster table.
        fetch_unclustered_gids_for_entity_mbids (function): Fetches the gids corresponding
                                                        to an entity_mbid that are not present
                                                        in entity_cluster table.
        get_entity_cluster_id_using_entity_mbids (function): Returns the entity_cluster_id
                                                            that corresponds to the given entity MBID.
        link_entity_mbids_to_entity_cluster_id (function): Links the entity mbid to the cluster_id.
        insert_entity_cluster (function): Creates a cluster with given cluster_id in the
                                        entity_cluster table.
        get_recordings_metadata_using_entity_mbid(function): gets recordings metadata using given MBID.

    Returns:
        clusters_modified (int): number of clusters modified.
        clusters_added_to_redirect (int): number of clusters added to redirect table.
    """

    logger = logging.getLogger(__name__)
    logger_level = logger.getEffectiveLevel()

    logger.debug("\nCreating clusters without considering anomalies...")
    clusters_modified = 0
    clusters_added_to_redirect = 0
    distinct_entity_mbids = fetch_unclustered_entity_mbids(connection)
    for entity_mbids in distinct_entity_mbids:
        gids = fetch_unclustered_gids_for_entity_mbids(connection, entity_mbids)
        if gids:
            cluster_id = get_entity_cluster_id_using_entity_mbids(connection, entity_mbids)
            if not cluster_id:
                cluster_id = gids[0]
                link_entity_mbids_to_entity_cluster_id(connection, cluster_id, entity_mbids)
                clusters_added_to_redirect +=1
            insert_entity_cluster(connection, cluster_id, gids)
            clusters_modified += 1

            if logger_level == logging.DEBUG:
                recordings = get_recordings_metadata_using_entity_mbid(connection, entity_mbids)
                _print_debug_info(connection, logger, cluster_id, gids, entity_mbids, recordings)

    logger.debug("\nClusters modified: {0}.".format(clusters_modified))
    logger.debug("Clusters added to redirect table: {0}.\n".format(clusters_added_to_redirect))
    logger.debug("=" * 80)

    return clusters_modified, clusters_added_to_redirect


def _print_debug_info(connection, logger, cluster_id, gids, entity_mbids, recordings):
    logger.debug("=" * 80)
    logger.debug("Cluster ID: {0}\n".format(cluster_id))
    if isinstance(entity_mbids, list):
        # Entity type is artist
        artists = mb_artist.get_many_artists_using_mbid(entity_mbids, includes=["comment"])
        logging.debug("Artist credit from MusicBrainz database:")
        artist_credit = ""
        for artist in artists.values():
            if artist['comment'] != '':
                logging.debug("{1} ({0})".format(artist['name'], artist['comment']))
            else:
                logging.debug("{0}".format(artist['name']))

        artist_titles = set()
        logging.debug("\nArtist credit added to the cluster based on artist MBIDs:")
        for recording in recordings:
            artist_title = recording.get("artist", "")
            artist_titles.add(artist_title)
        for artist_title in artist_titles:
            logging.debug("{0}".format(artist_title))
    else:
        # Entity type is release
        try:
            release = mb_release.get_release_by_mbid(entity_mbids, includes=["comment"])
            logging.debug("Release from MusicBrainz database:")
            if release['comment'] != '':
                logging.debug("{0} ({1})".format(release['name'], release['comment']))
            else:
                logging.debug("{0}".format(release['name']))

        except:
            pass

        release_titles = set()
        for recording in recordings:
            release_title = recording.get("release", "")
            release_titles.add("{0} ({1})".format(release_title, recording.get("release_mbid")))
        logging.debug("\nReleases added to the cluster based on release MBIDs:")
        for release_title in release_titles:
            logging.debug("{0}".format(release_title))

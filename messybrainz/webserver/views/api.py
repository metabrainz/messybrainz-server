import ujson
from flask import Blueprint, request, Response
from messybrainz import db
from messybrainz.db import artist as db_artist
from messybrainz.db import recording as db_recording
from messybrainz.db import release as db_release
from messybrainz.db.exceptions import BadDataException, NoDataFoundException
from messybrainz.webserver.decorators import crossdomain, ip_filter
from werkzeug.exceptions import BadRequest, NotFound
from uuid import UUID
import messybrainz
import messybrainz.db.exceptions
import ujson

REQUEST_TYPE = ['artist', 'recording', 'release']
INCLUDES = ['mbid']

api_bp = Blueprint('api', __name__)

def ujsonify(*args, **kwargs):
    """An implementation of flask's jsonify which uses ujson
    instead of json. Doesn't have as many bells and whistles
    (no indent/separator support).
    """
    return Response((ujson.dumps(dict(*args, **kwargs)), '\n'),
                        mimetype='application/json')

@api_bp.route("/submit", methods=["POST"])
@crossdomain()
@ip_filter
def submit():
    raw_data = request.get_data()
    try:
        data = ujson.loads(raw_data.decode("utf-8"))
    except ValueError as e:
        raise BadRequest("Cannot parse JSON document: %s" % e)

    if not isinstance(data, list):
        raise BadRequest("submitted data must be a list")

    try:
        result = messybrainz.submit_listens_and_sing_me_a_sweet_song(data)
        return ujsonify(result)
    except messybrainz.exceptions.BadDataException as e:
        raise BadRequest(e)


@api_bp.route("/<uuid:messybrainz_id>")
@crossdomain()
def get(messybrainz_id):
    try:
        data = messybrainz.load_recording(messybrainz_id)
    except messybrainz.exceptions.NoDataFoundException:
        raise NotFound

    return Response(ujson.dumps(data), mimetype='application/json')


@api_bp.route("/<uuid:messybrainz_id>/aka")
@crossdomain()
def get_aka(messybrainz_id):
    """Returns all other MessyBrainz recordings that are known to be equivalent
    (as specified in the clusters table).
    """
    raise NotImplementedError


@api_bp.route("/msid", methods=['GET'])
@crossdomain()
def get_msid_using_msid():
    """Returns all MessyBrainz IDs for the MSID in request."""

    msid = request.args.get('id')
    request_type = request.args.get('request_type')
    mbid = False
    includes = request.args.getlist('includes')
    if 'mbid' in includes:
        mbid = True

    data = {}
    try:
        if not validate_params(msid, request_type, includes):
            raise BadDataException
        if request_type == 'artist':
            with db.engine.begin() as connection:
                cluster_id = db_artist.get_cluster_id_using_msid(connection, msid)
                fetched_msids = db_artist.get_msids_using_cluster_id(connection, cluster_id)
                if mbid:
                    fetched_mbids = db_artist.get_artist_mbids_using_msid(connection, cluster_id)

        elif request_type == 'recording':
            with db.engine.begin() as connection:
                cluster_id = db_recording.get_recording_cluster_id_using_msid(connection, msid)
                fetched_msids = db_recording.get_gids_using_cluster_id(connection, cluster_id)
                if mbid:
                    fetched_mbids = db_recording.get_mbids_using_cluster_id(connection, cluster_id)
        elif request_type == 'release':
            with db.engine.begin() as connection:
                cluster_id = db_release.get_cluster_id_using_msid(connection, msid)
                fetched_msids = db_release.get_msids_using_cluster_id(connection, cluster_id)
                if mbid:
                    fetched_mbids = db_release.get_release_mbids_using_msid(connection, cluster_id)

        data = {'msid': fetched_msids}
        if mbid:
            data['mbid'] = fetched_mbids
    except BadDataException:
        raise BadRequest
    except NoDataFoundException:
        raise NotFound

    return Response(ujson.dumps(str(data)), mimetype='application/json')


@api_bp.route("/mbid", methods=['GET'])
@crossdomain()
def get_msid_using_mbid():
    mbids = request.args.getlist('ids')
    request_type = request.args.get('request_type')

    fetched_msids = []
    try:
        if not validate_params(mbids, request_type):
            raise BadDataException
        if request_type == 'artist':
            mbids.sort()
            mbids = [UUID(mbid) for mbid in mbids]
            with db.engine.begin() as connection:
                cluster_id = db_artist.get_artist_cluster_id_using_artist_mbids(connection, mbids)
                fetched_msids = db_artist.get_msids_using_cluster_id(connection, cluster_id)
        elif request_type == 'recording':
            with db.engine.begin() as connection:
                cluster_id = db_recording.get_recording_cluster_id_using_recording_mbid(connection, mbids[0])
                fetched_msids = db_recording.get_gids_using_cluster_id(connection, cluster_id)
        elif request_type == 'release':
            with db.engine.begin() as connection:
                cluster_id = db_release.get_release_cluster_id_using_release_mbid(connection, mbids[0])
                fetched_msids = db_release.get_release_mbids_using_msid(connection, cluster_id)
    except BadDataException:
        raise BadRequest
    except NoDataFoundException:
        raise NotFound

    return Response(ujson.dumps(str(fetched_msids)), mimetype='application/json')


def validate_params(gids, request_type, includes=None):
    if includes is None:
        includes = []

    if request_type not in REQUEST_TYPE:
        return False

    if includes:
        for inc in includes:
            if inc not in INCLUDES:
                return False
    if type(gids) is list:
        for gid in gids:
            if not is_valid_uuid(gid):
                return False
        return True
    else:
        return is_valid_uuid(gids)


# lifted from AcousticBrainz
def is_valid_uuid(u):
    try:
        u = UUID(u)
        return True
    except ValueError:
        return False

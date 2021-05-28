from flask import Blueprint, request, current_app, jsonify

from metacatalog import api, models

# create a blueprint
catalog = Blueprint('catalog', __name__)


# add catalog api function
@catalog.route('/uuid/<string:uuid>', methods=['GET'])
def get_uuid(uuid: str):
    """
    Return object of UUID
    """
    # get a database session
    session = current_app.config['session']

    # call api
    obj = api.get_uuid(session, uuid)

    # check istance
    if isinstance(obj, (models.Entry, models.EntryGroup)):
        return obj.export(path=None, fmt='JSON'), 200
    else:
        return jsonify(obj.to_dict()), 200

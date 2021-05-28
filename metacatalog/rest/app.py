from flask import Flask, jsonify
from flask_cors import CORS

from metacatalog import __version__
from metacatalog import ext

from .catalog import catalog

# create the app
app = Flask(__name__)
CORS(app=app, expose_headers='Authorization')

# placeholder for the session
app.config['session'] = None

# register blueprints
app.register_blueprint(catalog)

# DEV
@app.route('/')
def index():
    return jsonify({
        'message': 'Metadata rocks!',
        'version': __version__,
        'extensions': list(ext.EXTENSIONS.keys())
    }), 200

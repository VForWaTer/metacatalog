import os

__version__ = '0.2.8'
__author__ = 'Mirko Mälicke'

BASEPATH = os.path.abspath(os.path.dirname(__file__))
DATAPATH = os.path.join(BASEPATH, 'data')
USERDATAPATH = os.path.abspath(os.path.join(os.path.expanduser('~'), '.metacatalog', 'user_data'))
CONFIGFILE = os.path.join(os.path.expanduser('~'), '.metacatalog', 'config.json')

from metacatalog import api
from metacatalog import models
from metacatalog import db
from metacatalog import util
# ext has always to be the last import
from metacatalog import ext
from metacatalog.ext import extension
ext.__load_extensions()

import os

from .__version__ import __version__
__author__ = 'Mirko Mälicke'

BASEPATH = os.path.abspath(os.path.dirname(__file__))
DATAPATH = os.path.join(BASEPATH, 'data')

# import the config object
from metacatalog.config import config


from metacatalog import api
from metacatalog import models
from metacatalog import db
from metacatalog import util
from metacatalog import ext

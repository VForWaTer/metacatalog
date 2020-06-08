import os

__version__ = '0.1.9'
__author__ = 'Mirko Mälicke'

BASEPATH = os.path.abspath(os.path.dirname(__file__))
DATAPATH = os.path.join(BASEPATH, 'data')

from metacatalog import api
from metacatalog import models
from metacatalog import db
from metacatalog import util


#BASEPATH = os.path.abspath(os.path.dirname(__file__))

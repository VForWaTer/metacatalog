import os

from .db import Base, get_session, get_session
from .models import *

__version__ = '0.1.3'
__author__ = 'Mirko Mälicke'
BASEPATH = os.path.abspath(os.path.dirname(__file__))
DATAPATH = os.path.join(BASEPATH, 'data')
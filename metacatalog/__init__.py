import os

#from .db.base import Base
#from .db.session import get_session
#from .models import *

__version__ = '0.1.4'
__author__ = 'Mirko Mälicke'
BASEPATH = os.path.abspath(os.path.dirname(__file__))
DATAPATH = os.path.join(BASEPATH, 'data')
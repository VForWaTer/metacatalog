from .create import create
from .populate import populate

def init(args):
    # run create
    create(args)
    populate(args)

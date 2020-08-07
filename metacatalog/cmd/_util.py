import sys
from datetime import datetime

from metacatalog import __version__ as VERSION
from metacatalog.api import connect_database

def welcome():
    print("MetaCatalog management CLI (v%s)" % VERSION)


def empty():
    welcome()
    print("Nothing to do.\nRun with -h flag to get options.")


def connect(args):
    # check if a connection string was set
    conn = args.connection if args.connection is not None else 'default'
    echo = args.dev
    
    session = connect_database(conn, echo=echo)

    if args.verbose:
        cprint(args, 'Using session: %s' % str(session.bind))

    return session

def cprint(args, *print_args, **kwargs):
    """
    Wrapper around builtin print function. There is a bit of parameter name 
    overloads that might be renamed in the future.

    Parameters
    ----------
    args : argparse.NameSpace
        The argepase namespace to look for printing options
    print_args : list
        These are the args given to the builtin function for print
    kwargs : keyword arguments
        Overwrite print options. They will be passed to the 
        builtin print function. 
        **Do not overwrite the ``file`` argument.** 
    """
    # if quiet was passed, but logfile not, do not print anything
    if args.quiet and not args.logfile:
        return

    # if logfile is given
    if args.logfile:
        lf = args.logfile
        cmds = ['[{date}]:\t'.format(date=datetime.utcnow().isoformat()), *print_args]
    else:
        lf = sys.stdout
        cmds = [*print_args]

    # print 
    print(*cmds, file=lf, **kwargs)
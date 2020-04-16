import argparse
import codecs

def unescaped(arg_str):
    return codecs.decode(str(arg_str), 'unicode_escape')

from metacatalog import __version__ as VERSION
from metacatalog.cmd import (
    empty, 
    create, 
    populate, 
    connection, 
    init,
    find,
    show,
    add
)

def main():
    default_options = argparse.ArgumentParser(add_help=False)
    default_options.add_argument("--version", "-v", action="store_true", help="Returns the module version")
    default_options.add_argument("--connection", "-C",  type=str, help="Connection string to the database instance.Follows the syntax:\ndriver://user:password@host:port/database")
    default_options.add_argument("--verbose", "-V", action="store_true", help="Activate extended output.")

    # build the main Argument parser
    parser = argparse.ArgumentParser(description="MetaCatalog management CLI", add_help=True)
#    parser.add_argument("--version", "-v", action="store_true", help="Returns the module version")
#    parser.add_argument("--connection", "-C",  type=str, help="Connection string to the database instance.Follows the syntax:\ndriver://user:password@host:port/database")
#    parser.add_argument("--verbose", "-V", action="store_true", help="Activate extended output.")
    parser.set_defaults(func=empty)

    # add subparsers
    subparsers = parser.add_subparsers(title="Commands", description="CLI commands", dest='func')

    # create parser
    create_parser = subparsers.add_parser('create', parents=[default_options], add_help=True,  help="Create a new Metacatalog instance.")
    create_parser.set_defaults(func=create)

    # populate parser
    pop_parser = subparsers.add_parser('populate', parents=[default_options], add_help=True, help="Populate the database with default auxiliary data.")
    pop_parser.add_argument("--ignore", "-I", nargs='+', help="List tables to be ignored for default population.")
    pop_parser.set_defaults(func=populate)

    # init parser
    init_parser = subparsers.add_parser('init', parents=[pop_parser], add_help=False, help="Runs the create and and the populate command.")
    init_parser.set_defaults(func=init)

    # connection parser
    conn_parser = subparsers.add_parser('connection', parents=[default_options], add_help=True, help="Manage stored connections")
    conn_parser.add_argument("--save", help="Saves the given connection string. Follows the syntax:\ndriver://user:password@host:port/database")
    conn_parser.add_argument("--name", help="If used with --save, specifies the name for the connection string. Else, only this string will be returned.")
    conn_parser.set_defaults(func=connection)

    # find parser
    find_parser = subparsers.add_parser('find', parents=[default_options], add_help=True, help="Find records in the database on exact matches.")
    find_parser.add_argument('entity', type=str, help="Name of the requested database entity.")
    find_parser.add_argument('--by', type=str, action="append", nargs=2, help="key value pair to be used for finding record(s) in the database. Flag can be used multiple times.")
    find_parser.set_defaults(func=find)

    # show subparsers
    show_parser = subparsers.add_parser('show',parents=[default_options], add_help=True, help="Show database structure or records.")
    show_parser.add_argument('action', choices=['attributes', 'records'], help="Element to be shown.\nattributes\tShow table attributes.\nrecords\tShow raw table records")
    show_parser.add_argument('table', help="Table name.")
    show_parser.add_argument('--names-only', dest="name_only", action="store_true", default=False, help="Show only the attribute names. Only valid with 'attribute' action.")
    show_parser.add_argument('--limit', '-L', type=int, help="Only valid with 'records' action. Will limit the number of records returned")
    show_parser.add_argument('--where', type=str, help="Only valid with 'records' action. Raw SQL WHERE clause to filter the results. Use carefully.")
    show_parser.add_argument('--truncate', '-T', action="store_true", help="Only valid with 'records' action. Truncates string output to 12 signs.")

    show_parser.set_defaults(func=show)

    # add parser
    add_parser = subparsers.add_parser('add', parents=[default_options], add_help=True, help="Add new records to the database.\nHas to be combined with one of the data origin flags.")
    add_parser.add_argument('entity', type=str, help="Name of the record entity to be added.")
    add_parser.add_argument('--csv', type=unescaped, help="Data Origin Flag. Pass a CSV filename or content containing the data. Column header have to match the ADD API keywords.")
    add_parser.add_argument('--txt', type=unescaped, help="Data Origin Flag. Pass a text filename or content containing whitespace separated key=value pairs where key has to match the ADD API keywords. If used directly remember to quote accordingly.")
    add_parser.add_argument('--json', type=unescaped, help="Data Origin Flag. Pass a JSON filename or content containing the data. Must contain a list of objects matchin the ADD API keywords.")
    add_parser.set_defaults(func=add)

    # parse the arguments
    args = parser.parse_args()

    if hasattr(args, 'version') and args.version:
        print(VERSION)
    else:
        args.func(args)

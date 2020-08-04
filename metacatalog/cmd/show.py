from tabulate import tabulate

from metacatalog.api import show_attributes, show_records
from ._util import connect, cprint


def show(args):
    if args.action == 'attributes':
        show_attr(args)
    if args.action == 'records':
        show_recs(args)
    else:
        cprint(args, "'%s' is not supported." % args.action)

def show_attr(args):
    # the attributes command does not need a session
    # as the attributes are infered from the ORM Model
    table_name = args.table
    name_only = args.name_only if args.name_only is not None else False

    # get the list
    attribute_list = show_attributes(table_name, add_type=not name_only)

    # create output
    message = "Attributes of %s" % table_name
    cprint(args, message)
    cprint(args, '-' * len(message))
    if name_only:
        cprint(args, '\n'.join(attribute_list))
    else:
        for attr in attribute_list:
            cprint(args, '%s  |  %s' % (attr[0], attr[1]))


def show_recs(args):
    # get a session to the database
    session = connect(args)

    # get the table name
    table_name = args.table

    # get the optional arguments
    limit = args.limit
    where = args.where
    trunc = args.truncate

    # get the attribute list
    attribute_list = show_attributes(table_name, add_type=False)

    # get the records
    records = show_records(session, table_name=table_name, limit=limit, where=where, as_dict=False)

    if trunc:
        records = [[r[:12] + '...' if isinstance(r, str) else r for r in rec] for rec in records]

    cprint(args, tabulate(records, headers=attribute_list))
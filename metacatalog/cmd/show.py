from metacatalog.api import show_attributes


def show(args):
    if args.action == 'attributes':
        show_attr(args)

def show_attr(args):
    # the attributes command does not need a session
    # as the attributes are infered from the ORM Model
    table_name = args.table
    name_only = args.name_only if args.name_only is not None else False

    # get the list
    attribute_list = show_attributes(table_name, add_type=not name_only)

    # create output
    message = "Attributes of %s" % table_name
    print(message)
    print('-' * len(message))
    if name_only:
        print('\n'.join(attribute_list))
    else:
        for attr in attribute_list:
            print('%s  |  %s' % (attr[0], attr[1]))
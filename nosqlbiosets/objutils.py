""" Methods to update objects for better representation in databases """


# Make sure type of list attributes are list
def unifylistattributes(e, list_attrs):
    for listname in list_attrs:
        if listname not in e:
            continue
        objname = listname[:-1]
        if e[listname] is None:
            del e[listname]
        else:
            if isinstance(e[listname][objname], list):
                e[listname] = e[listname][objname]
            else:
                e[listname] = [e[listname][objname]]

""" Methods to update objects for better data representation in databases """


def _guessobjname(listname):
    """
    categories --> category
    transporters --> transporter
    """
    if 'ries' == listname[-4:]:
        objname = listname[:-3] + 'y'
    else:  # assume list name ends with 's'
        objname = listname[:-1]
    return objname


# Make sure type of list attributes are list
def unifylistattributes(e, list_attrs):
    for listname in list_attrs:
        if listname not in e:
            continue
        objname = _guessobjname(listname)
        if e[listname] is None:
            del e[listname]
        else:
            if isinstance(e[listname][objname], list):
                e[listname] = e[listname][objname]
            else:
                e[listname] = [e[listname][objname]]


# Make sure type of boolean attributes are boolean
def checkbooleanattributes(e, attrs):
    for attr in attrs:
        if attr in e:
            if not isinstance(e[attr], bool):
                if e[attr] in ['true', 'True']:
                    e[attr] = True
                else:
                    e[attr] = False

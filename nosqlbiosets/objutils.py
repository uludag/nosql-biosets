""" Methods to update objects for better data representation in databases.

 xmltodict library is a reliable library that most of the xml datasets supported
 by nosqlbiosets project are parsed with.
 For various reasons source xml files may have extra layers of names, especially
 for list attributes, e.g. obj->genes->gene. Here unifylistattribute method
 remove the last name 'gene' to simplify browsing and querying the data
 from the databases.
 """


# Make sure type of given list attribute is list
def unifylistattribute(e, listname, objname):
    if e is None:
        return
    if listname in e:
        if e[listname] is None:
            del e[listname]
        else:
            if isinstance(e[listname][objname], list):
                e[listname] = e[listname][objname]
            else:
                e[listname] = [e[listname][objname]]


# Make sure type of given list attributes are list
# List attribute names are assumed to end with 's', and object names
# are equal to the list names without 's', such as genes vs gene
def unifylistattributes(e, list_attrs):
    for listname in list_attrs:
        objname = listname[:-1]
        unifylistattribute(e, listname, objname)


# Make sure type of boolean attributes are boolean
def checkbooleanattributes(e, attrs):
    if e is None:
        return
    for attr in attrs:
        if attr in e:
            if not isinstance(e[attr], bool):
                if e[attr] in ['true', 'True']:
                    e[attr] = True
                else:
                    e[attr] = False

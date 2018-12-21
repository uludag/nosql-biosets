import json


def parseinputquery(query):
    """ Checks naively whether the input could be a MongoDB query-clause
        If not returns a MongoDB text search query-caluse with given input
        being the search term
    """
    qc = None
    if isinstance(query, dict):
        qc = query
    else:
        try:
            qc = json.loads(query)
        except ValueError:
            pass
        finally:
            if qc is None:
                qc = {"$text": {"$search": query}}
    return qc

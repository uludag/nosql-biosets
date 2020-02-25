import json

from nosqlbiosets.dbutils import DBconnection


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


class Query:

    def __init__(self, dbtype, index, mdbcollection, **kwargs):
        self.index = index
        self.mdbcollection = mdbcollection
        self.dbc = DBconnection(dbtype, self.index, **kwargs)

    def query(self, qc, projection=None, limit=0):
        if self.dbc.db == 'Elasticsearch':
            c = self.dbc.es.search(index=self.index, body=qc, size=limit)
        else:
            c = self.dbc.mdbi[self.mdbcollection].find(qc,
                                                       projection=projection,
                                                       limit=limit)
        return c

    def count(self, qc, **kwargs):
        if self.dbc.db == 'Elasticsearch':
            n = self.dbc.es.count(index=self.dbc.index, body=qc)['count']
        else:
            n = self.dbc.mdbi[self.mdbcollection].count(qc, **kwargs)
        return n

    def distinct(self, key, qc=None):
        r = self.dbc.mdbi[self.mdbcollection].distinct(key, filter=qc)
        return r

    def aggregate_query(self, agpl, **kwargs):
        r = self.dbc.mdbi[self.mdbcollection].aggregate(agpl, **kwargs)
        return r

    def esquery(self, index, qc, size=10):
        import json
        print("Querying '%s': %s" % (index, json.dumps(qc, indent=4)))
        es = DBconnection("Elasticsearch", index).es
        r = es.search(index=index, body=qc, size=size)
        nhits = r['hits']['total']
        aggs = r["aggregations"] if "aggregations" in r else None
        return r['hits']['hits'], nhits, aggs

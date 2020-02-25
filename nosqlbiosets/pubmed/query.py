""" Queries with PubMed data indexed with Elasticsearch """

from nosqlbiosets.qryutils import Query


class QueryPubMed(Query):

    # Check whether ids have already been indexed
    def checkpubmedidsindexed(self, ids):
        assert self.dbc.db == 'Elasticsearch'
        qry = {
            "query": {"ids": {"values": ids}}
        }
        r = self.count(qry)
        return r == len(ids)

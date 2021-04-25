""" Queries with PubMed data indexed with Elasticsearch """

from nosqlbiosets.qryutils import Query


class QueryPubMed(Query):

    # Check whether ids have already been indexed
    def checkpubmedidsindexed(self, ids):
        if self.dbc.db == 'Elasticsearch':
            qry = {
                "query": {"ids": {"values": ids}}
            }
            r = self.count(qry)
            # 200 is an arbitrary number
            # that represents the maximum number of entries that could have been
            # deleted with the later update files,    need to be improved
            return r >= len(ids)-200
        else:
            return False

    # Delete PubMed records that have been marked as deleted
    def deletepubmedids(self, ids):
        if self.dbc.db == 'Elasticsearch':
            qry = {
                "query": {"ids": {"values": ids}}
            }
            self.dbc.es.delete_by_query(index=self.dbc.index, body=qry)

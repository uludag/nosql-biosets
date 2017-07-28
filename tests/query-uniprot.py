#!/usr/bin/env python
""" Simple sample queries with UniProt data
 indexed with MongoDB and Elasticsearch """

import json
import os
import unittest

from nosqlbiosets.dbutils import DBconnection

# Some query ideas:
# dbReference.type:KEGG  comment.type.keyword:pathway
# comment.type.keyword:"enzyme regulation"simple
# comment.type.keyword:"catalytic activity"
# gene.name.type: "ORF"


def query(es, index, qc, doc_type=None, size=0):
    print("querying '%s'  %s" % (doc_type, str(qc)))
    r = es.search(index=index, doc_type=doc_type,
                  body={"size": size, "query": qc})
    nhits = r['hits']['total']
    return r['hits']['hits'], nhits


class QueryUniProt(unittest.TestCase):
    d = os.path.dirname(os.path.abspath(__file__))
    index = "uniprot"
    doctype = "protein"

    def query_sample_keggids(self, dbc, cids):
        if dbc.db == 'Elasticsearch':
            qc = {"match": {"dbReference.id": "(\"%s\")" % '" OR "'.join(cids)}}
            hits, n = query(dbc.es, self.index, qc, self.doctype, len(cids))
            mids = [xref['_id'] for xref in hits]
        else:  # MongoDB
            qc = {'dbReference.id': {'$in': cids}}
            print(qc)
            hits = dbc.mdbi[self.doctype].find(qc, limit=10)
            mids = [c['_id'] for c in hits]
        print(mids)
        return mids

    def queries(self, db):
        dbc = DBconnection(db, self.index)
        mids = self.query_sample_keggids(dbc, ['hsa:7157', 'hsa:121504'])
        self.assertEqual(mids, ['P53_HUMAN', 'H4_HUMAN'])

    def aggs(self, db):
        dbc = DBconnection(db, self.index)
        doctype = "protein"
        aggfl = "$comment.type"
        if db == "MongoDB":
            agpl = [
                {"$match": {"organism.name.#text": "Human"}},
                {"$unwind": aggfl},
                {"$group": {"_id": aggfl,
                            "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 10}
            ]
            # some query ideas:
            # agpl = [
            #     {"$facet":{
            #         "categorizedbygenetype": [
            #             {"$sortByCount": "$gene.name.type"}]}}]
            # hits = dbc.mdbi[doctype].distinct("gene.name.type")

            hits = dbc.mdbi[doctype].aggregate(agpl)
            l = [i for i in hits]
        else:  # "Elasticsearch"
            qc = {
                "_source": "*.type",
                "query": {
                    "match": {
                        "organism.dbReference.id": "9606"}},
                "aggs": {
                    "tids": {
                        "terms": {
                            "field": "comment.type.keyword",
                            "size": 10}}}
            }
            hits, n, l = self.esquery(dbc.es, self.index, qc, doctype)
        print(json.dumps(l, indent=2))

    @staticmethod
    def esquery(es, index, qc, doc_type=None):
        print("querying '%s'  %s" % (doc_type, str(qc)))
        r = es.search(index=index, doc_type=doc_type,
                      body=qc)
        nhits = r['hits']['total']
        return r['hits']['hits'], nhits, r["aggregations"]

    def test_queries(self):
        self.queries("Elasticsearch")
        self.queries("MongoDB")

    def __test_all(self):
        self.aggs("MongoDB")
        self.aggs("Elasticsearch")


if __name__ == '__main__':
    unittest.main()

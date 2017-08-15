#!/usr/bin/env python
""" Simple queries with KEGG pathways """

import unittest

from nosqlbiosets.dbutils import DBconnection


class QueryKEGGpathway(unittest.TestCase):
    index = "kegg-tests"
    doctype = "kegg_pathway"
    mdb = DBconnection("MongoDB", index).mdbi
    es = DBconnection("Elasticsearch", index).es

    def es_query(self, qc, size=0):
        print("querying '%s'  %s" % (self.doctype, str(qc)))
        aggs = {
            "titles": {
                "terms": {
                    "field": "title.keyword",
                    "size": 10
                }
            }
        }
        r = self.es.search(index=self.index, doc_type=self.doctype,
                           body={"size": size, "query": qc, "aggs": aggs})
        nhits = r['hits']['total']
        return r['aggregations']['titles']['buckets'], nhits

    def mdb_query(self, qc, doctype=None, size=20):
        print("Querying %s  %s" % (doctype, str(qc)))
        c = self.mdb[doctype].find(qc, limit=size)
        r = [doc for doc in c]
        c.close()
        return r

    # Return list of pathways with given compound id
    def query_sample_keggid(self, l, db):
        if db == 'Elasticsearch':
            qc = {"match": {"entry.name": 'cpd:'+l}}
            titles, _ = self.es_query(qc, size=10)
            titles = [c['key'] for c in titles]
        else:  # MongoDB
            qc = {"entry.name": "cpd:"+l}
            hits = self.mdb_query(qc, self.doctype)
            titles = [c['title'] for c in hits]
        return titles

    def test_queries(self):
        for db in ["Elasticsearch", "MongoDB"]:
            mids = self.query_sample_keggid('C05379', db)
            self.assertIn('2-Oxocarboxylic acid metabolism', mids)

if __name__ == '__main__':
    unittest.main()

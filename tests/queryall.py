#!/usr/bin/env python
""" Sample queries with multiple 'nosql-biosets' indexes """

import json
import os
import unittest

from elasticsearch import Elasticsearch

typeaggs = {
    "type": {
        "terms": {
            "field": "_type",
            "size": 10}}}


class QueryallTests(unittest.TestCase):
    conf = {"host": "localhost", "port": 9200}
    d = os.path.dirname(os.path.abspath(__file__))
    try:
        conf = json.load(open(d + "/../conf/elasticsearch.json", "r"))
    finally:
        pass
    es = Elasticsearch(host=conf['host'], port=conf['port'], timeout=600)

    def query(self, qterms, doctype=None):
        qc = {"match": {"_all": ' AND '.join(qterms)}}
        r = self.es.search(doc_type=doctype,
                           body={"size": 0, "query": qc, 'aggs': typeaggs})
        return r

    def querywithtypes(self, qterms):
        r = self.query(qterms)
        types = r['aggregations']['type']['buckets']
        self.assertGreater(len(types), 1)
        for t in types:
            doctype = t['key']
            r = self.query(qterms, doctype=doctype)
            self.assertEqual(r['hits']['total'], t['doc_count'])

    def test_queries(self):
        """Makes a query with given query terms, grouping the results
        with document types, number of results are then compared by repeating
        the same query with specifying document type with each type"""
        l = [["p53", "kinase"], ["xylose"], ["naringenin"], ["acetyl"]]
        for qterms in l:
            self.querywithtypes(qterms)


if __name__ == '__main__':
    unittest.main()

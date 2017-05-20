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
            "size": 50,
            "order": {
                "_count": "desc"
            }}}}


class QueryallTests(unittest.TestCase):
    conf = {"host": "localhost", "port": 9200}
    d = os.path.dirname(os.path.abspath(__file__))
    try:
        conf = json.load(open(d + "/../conf/elasticsearch.json", "r"))
    finally:
        pass
    es = Elasticsearch(host=conf['host'], port=conf['port'], timeout=600)
    index = ""

    def query(self, qterms, doctype=None):
        qc = {"match": {"_all": ' '.join(qterms)}}
        r = self.es.search(doc_type=doctype,
                           body={"size": 0, "query": qc, 'aggs': typeaggs})
        return r

    def test_query(self):
        """Makes a sample query with sample query term 'p53',
        grouping results with document types
        then the number of results are checked by repeating
        the same query with specifying document type with each type"""
        qterms = ['p53']
        r = self.query(qterms)
        types = r['aggregations']['type']['buckets']
        self.assertGreater(len(types), 1)
        for t in types:
            doctype = t['key']
            print("doctype: %s" % doctype)
            r = self.query(qterms, doctype=doctype)
            self.assertEqual(r['hits']['total'], t['doc_count'])


if __name__ == '__main__':
    unittest.main()

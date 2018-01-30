#!/usr/bin/env python
""" Test queries with multiple 'nosql-biosets' Elasticsearch indexes """

import unittest

from nosqlbiosets.dbutils import DBconnection

typeaggs = {
    "type": {
        "terms": {
            "field": "_type",
            "size": 10}}}


class QueryallTests(unittest.TestCase):
    es = DBconnection('Elasticsearch', "*").es

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
        qtermsl = [["p53", "kinase"], ["xylose"], ["naringenin"], ["acetyl"]]
        for qterms in qtermsl:
            self.querywithtypes(qterms)


if __name__ == '__main__':
    unittest.main()

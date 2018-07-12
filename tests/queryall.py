#!/usr/bin/env python
""" Test queries with multiple 'nosql-biosets' Elasticsearch indexes.
Work in this file is a stub.
"""

import unittest

from nosqlbiosets.dbutils import DBconnection

groupbyindex = {
    "indx": {
        "terms": {
            "field": "_index",
            "size": 10}}}


class Queryall:
    es = DBconnection('Elasticsearch', "*").es

    def queryterms(self, qterms, aggs, index=None, size=10):
        qc = {
            "query_string": {
                "query": ' AND '.join(qterms)}}
        r = self.es.search(index=index,
                           body={"size": size, "query": qc, 'aggs': aggs})
        return r


def querytermsgroupbyindex(qterms):
    qryall = Queryall()
    r = qryall.queryterms(qterms, aggs=groupbyindex, size=0)
    buckets = r['aggregations']['indx']['buckets']
    assert 1 <= len(buckets)
    print(qterms)
    for b in buckets:
        index = b['key']
        n = b['doc_count']
        print("%s: %d" % (index, n))
    print("Total: %d" % (r['hits']['total']))
    return r


class QueryallTests(unittest.TestCase):
    qryall = Queryall()
    qtermsl = [["p53", "kinase"], ["xylose"], ["naringenin"], ["acetyl"]]

    def test_querytermsgroupbyindex(self):
        for qterms in self.qtermsl:
            r = querytermsgroupbyindex(qterms)
            assert r is not None

    def test_queryterms(self):
        """Query with sample query terms, grouping the results
        with indexes, number of results are then compared by repeating
        the same query with specifying index for each result group"""
        for qterms in self.qtermsl:
            r = self.qryall.queryterms(qterms, aggs=groupbyindex, size=0)
            buckets = r['aggregations']['indx']['buckets']
            self.assertGreater(len(buckets), 1)
            for t in buckets:
                index = t['key']
                r = self.qryall.queryterms(qterms, aggs={}, index=index, size=0)
                assert t['doc_count'] == r['hits']['total'], qterms

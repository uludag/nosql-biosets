#!/usr/bin/env python3
""" Sample queries for PubTator annotations """

import json
import os
import unittest

from elasticsearch import Elasticsearch


class QueryPubTator(unittest.TestCase):
    conf = {"host": "localhost", "port": 9200}
    d = os.path.dirname(os.path.abspath(__file__))
    try:
        conf = json.load(open(d + "/../conf/elasticsearch.json", "r"))
    finally:
        pass
    es = Elasticsearch(host=conf['host'], port=conf['port'], timeout=600)
    index = "pubtator"

    def query(self, qc, aggqc):
        print("querying %s with aggregations %s" % (str(qc), str(aggqc)))
        r = self.es.search(index=self.index,
                           body={"size": 0, "query": qc, "aggs": aggqc})
        return r

    def test_query_sample_geneids(self):
        l = [652, 17906, 39014]
        for gid in l:
            qc = {"match": {"geneids": gid}}
            n = self.query(qc, {})['hits']['total']
            self.assertGreater(n, 0, "No annotation found for gene %d" % gid)

    def test_sample_aggregation_queries(self):
        # top resources and their top mentions
        qc = {"match_all": {}}
        aggqc = {
            "resources": {
                "terms": {
                    "field": "resource",
                    "size": 10},
                "aggs": {
                    "mentions": {
                        "terms": {
                            "field":
                                "mentions.keyword",
                            "size": 4
                        }}}}}
        r = self.query(qc, aggqc)
        self.assertGreater(r['hits']['total'], 4000,
                           "Less than expected number of annotations")
        dc0 = r['aggregations']['resources']['buckets'][0]['doc_count']
        self.assertGreater(dc0, 1000,
                           "Less than expected number of annotations")


if __name__ == '__main__':
    unittest.main()

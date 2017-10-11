#!/usr/bin/env python3
""" Simple queries with PubTator annotations """

import unittest

from nosqlbiosets.dbutils import DBconnection


class QueryPubTator(unittest.TestCase):
    index = "pubtator"
    dbc = DBconnection("Elasticsearch", index)

    def query(self, qc, aggqc):
        print("Querying %s with aggregations %s" % (str(qc), str(aggqc)))
        r = self.dbc.es.search(index=self.index,
                               body={"size": 0, "query": qc, "aggs": aggqc})
        return r

    def test_query_sample_geneids(self):
        geneids = [652, 17906, 39014]
        for gid in geneids:
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
        self.assertGreater(r['hits']['total'], 1400,
                           "Less than expected number of annotations")
        dc0 = r['aggregations']['resources']['buckets'][0]['doc_count']
        self.assertGreater(dc0, 1000,
                           "Less than expected number of annotations")


if __name__ == '__main__':
    unittest.main()

#!/usr/bin/env python3
""" Sample queries for PubTator annotations """

import argparse

from elasticsearch import Elasticsearch


class Tests:
    def __init__(self, es, index, doc_type):
        self.es = es
        self.index = index
        self.doc_type = doc_type

    def aggquery(self, qc, aggqc):
        print("querying %s with aggregations %s" % (str(qc), str(aggqc)))
        r = self.es.search(index=self.index,
                           body={"size": 0, "query": qc, "aggs": aggqc})
        return r

    def query_sample_geneids(self):
        l = [652, 17906, 39014]
        for gid in l:
            qc = {"match": {"geneid": gid}}
            n = self.aggquery(qc, {})
            if n == 0:
                print("gene2pub annot with id %d not found" % gid)

    def sample_aggregation_queries(self):
        # top resources and their top mentions
        qc = { "match_all": {}}
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
        r = self.aggquery(qc, aggqc)
        if r['hits']['total'] < 4000:
            print("less than expected number of annotations")
        if r['aggregations']['resources']['buckets'][0]['doc_count'] < 10000:
            print("less than expected number of annotations")


def main(es, index):
    tests = Tests(es, index, "bioassay")
    tests.query_sample_geneids()
    tests.sample_aggregation_queries()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Query PubTator Elasticsearch index')
    parser.add_argument('--index',
                        default="pubtator-test1",
                        help='name of the elasticsearch index')
    parser.add_argument('--host', default="localhost",
                        help='Elasticsearch server hostname')
    parser.add_argument('--port', default="9200",
                        help="Elasticsearch server port")
    parser.add_argument('--debug', default=False,
                        help="print more information")
    args = parser.parse_args()
    host = args.host
    port = args.port
    debug = args.debug
    con = Elasticsearch(host=host, port=port, timeout=600)
    main(con, args.index)

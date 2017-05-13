#!/usr/bin/env python
""" Sample queries with Metanetx compounds and reactions """

import argparse
import json
import os

from elasticsearch import Elasticsearch


def query(es, index, qc, doc_type=None, size=0):
    print("querying %s  %s" % (doc_type, str(qc)))
    r = es.search(index=index, doc_type=doc_type,
                  body={"size": size, "query": qc})
    nhits = r['hits']['total']
    return r['hits']['hits'], nhits


def aggquery(es, index, qc, aggqc):
    print("querying %s with aggregations" % str(qc))
    r = es.search(index=index,
                  body={"size": 0, "query": qc, "aggs": aggqc})
    return r


class Tests:
    def __init__(self, es, index):
        self.es = es
        self.index = index

    def query_sample_keggids(self, l):
        qc = {"match": {"xrefs": ' '.join(l)}}
        hits, n = query(self.es, self.index, qc, "compound", len(l))
        print(n)
        print("compound with id %s  found=%d" % (len(l), n))
        mids = [xref['_id'] for xref in hits]
        print(mids)
        return mids

    def query_sample_metanetxids(self, mids):
        qc = {"ids": {"type": "compound", "values": mids}}
        hits, n = query(self.es, self.index, qc, "compound", len(mids))
        print(n)
        print("compound with id %s  found=%d" % (len(mids), n))
        descs = [c['_source']['desc'] for c in hits]
        print(descs)
        return descs


def main(es, index):
    tests = Tests(es, index)
    mids = tests.query_sample_keggids(['C00116', 'C05433'])
    print(mids == ['MNXM2000', 'MNXM89612'])
    descs = tests.query_sample_metanetxids(mids)
    print(descs == ['glycerol', 'alpha-carotene'])


if __name__ == '__main__':
    conf = {"host": "localhost", "port": 9200}
    d = os.path.dirname(os.path.abspath(__file__))
    try:
        conf = json.load(open(d + "/../conf/elasticsearch.json", "r"))
    finally:
        pass
    parser = argparse.ArgumentParser(
        description='Query MetaNetX Elasticsearch index with sample queries')
    parser.add_argument('--index',
                        default="metanetx-0.2",
                        help='Name of the elasticsearch index')
    parser.add_argument('--host', default=conf['host'],
                        help='Elasticsearch server hostname')
    parser.add_argument('--port', default=conf['port'],
                        help="Elasticsearch server port")
    args = parser.parse_args()
    con = Elasticsearch(host=args.host, port=args.port, timeout=600)
    main(con, args.index)

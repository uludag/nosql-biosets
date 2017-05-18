#!/usr/bin/env python
""" Sample queries with MetaNetX compounds and reactions """

import json
import os
import unittest

from elasticsearch import Elasticsearch


def query(es, index, qc, doc_type=None, size=0):
    print("querying %s  %s" % (doc_type, str(qc)))
    r = es.search(index=index, doc_type=doc_type,
                  body={"size": size, "query": qc})
    nhits = r['hits']['total']
    return r['hits']['hits'], nhits


class QueryMetanetx(unittest.TestCase):
    conf = {"host": "localhost", "port": 9200}
    d = os.path.dirname(os.path.abspath(__file__))
    try:
        conf = json.load(open(d + "/../conf/elasticsearch.json", "r"))
    finally:
        pass
    es = Elasticsearch(host=conf['host'], port=conf['port'], timeout=600)
    index = "metanetx-0.2"

    def query_sample_keggids(self, l):
        qc = {"match": {"xrefs": ' '.join(l)}}
        hits, n = query(self.es, self.index, qc, "compound", len(l))
        mids = [xref['_id'] for xref in hits]
        print(mids)
        return mids

    def query_sample_metanetxids(self, mids):
        qc = {"ids": {"type": "compound", "values": mids}}
        hits, n = query(self.es, self.index, qc, "compound", len(mids))
        descs = [c['_source']['desc'] for c in hits]
        print(descs)
        return descs

    def test(self):
        mids = self.query_sample_keggids(['C00116', 'C05433'])
        self.assertEqual(mids, ['MNXM2000', 'MNXM89612'])
        descs = self.query_sample_metanetxids(mids)
        self.assertEqual(descs, ['glycerol', 'alpha-carotene'])


if __name__ == '__main__':
    unittest.main()

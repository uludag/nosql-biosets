#!/usr/bin/env python
""" Sample queries with KEGG pathway """

import unittest

from nosqlbiosets.dbutils import DBconnection


class QueryKEGGpathway(unittest.TestCase):
    index = "biosets"
    mdb = DBconnection("MongoDB", index).mdbi

    def query(self, qc, doctype=None, size=20):
        print("Querying %s  %s" % (doctype, str(qc)))
        c = self.mdb[doctype].find(qc, limit=size)
        r = [doc for doc in c]
        c.close()
        return r

    # Return list of pathways with given compound
    def query_sample_keggid(self, l):
        qc = {"entry.name": "cpd:"+l}
        hits = self.query(qc, "pathway")
        mids = [c['title'] for c in hits]
        print(mids)
        return mids

    def test_queries(self):
        mids = self.query_sample_keggid('C05379')
        self.assertIn('2-Oxocarboxylic acid metabolism', mids)

if __name__ == '__main__':
    unittest.main()

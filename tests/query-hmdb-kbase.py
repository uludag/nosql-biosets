#!/usr/bin/env python
""" Sample queries with HMDB and Kbase metabolites, proteins, compounds
 and reactions data"""

import unittest

from nosqlbiosets.dbutils import DBconnection


class QueryHMDB(unittest.TestCase):
    index = "biosets"
    indxr = DBconnection("MongoDB", index)
    mdb = indxr.mdbi

    def query(self, qc, doctype=None, size=20):
        print("Querying %s  %s" % (doctype, str(qc)))
        c = self.mdb[doctype].find(qc, limit=size)
        r = [doc for doc in c]
        c.close()
        return r

    def query_sample_keggids(self, l):
        qc = {"keggid": ' '.join(l)}
        hits = self.query(qc, "compound")
        mids = [c['_id'] for c in hits]
        print(mids)
        return mids

    def query_synoyms(self, l):
        qc = {"synonym": ' '.join(l)}
        hits = self.query(qc, "compound")
        mids = [c['_id'] for c in hits]
        print(mids)
        return mids

    def text_search(self, doctype, l):
        qc = {'$text': {'$search': ' '.join(l)}}
        hits = self.query(qc, doctype)
        mids = [c['_id'] for c in hits]
        print(mids)
        return mids

    def test_aggregate_query(self):
        doctype = 'metabolite'
        agpl = [
            {'$match': {'$text': {'$search': 'ATP'}}},
            {'$group': {
                '_id': '$taxonomy.super_class', "count": {"$sum": 1}}}
        ]
        hits = self.mdb[doctype].aggregate(agpl)
        print()
        mids = [c['_id'] for c in hits]
        self.assertIn('Organoheterocyclic compounds', mids)
        print(mids)

    def test_queries(self):
        mids = self.query_sample_keggids(['C00473'])
        self.assertEqual(mids, ['cpd00365'])
        mids = self.query_synoyms(['ATP'])
        self.assertEqual(mids, ['cpd00002'])
        mids = self.text_search('metabolite', ['ATP'])
        self.assertEqual(len(mids), 20)


if __name__ == '__main__':
    unittest.main()

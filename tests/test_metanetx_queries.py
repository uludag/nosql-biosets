#!/usr/bin/env python
""" Sample queries with MetaNetX compounds and reactions """

import os
import unittest

from nosqlbiosets.dbutils import DBconnection
from nosqlbiosets.metanetx.query import QueryMetaNetX
from nosqlbiosets.uniprot.query import QueryUniProt

qrymtntx = QueryMetaNetX()
qryuniprot = QueryUniProt()


class TestQueryMetanetx(unittest.TestCase):
    d = os.path.dirname(os.path.abspath(__file__))
    index = "biosets"

    def test_compound_desc(self, db="MongoDB"):
        dbc = DBconnection(db, self.index)
        mids = ['MNXM39', 'MNXM89612']
        descs = qrymtntx.query_metanetxids(dbc, mids)
        self.assertEqual(set(descs), {'formate', 'glycerol'})

    def id_queries(self, db):
        dbc = DBconnection(db, self.index)
        mids = qrymtntx.keggcompoundids2metanetxids(dbc, ['C00116', 'C05433'])
        self.assertEqual(mids, ['MNXM2000', 'MNXM89612'])
        descs = qrymtntx.query_metanetxids(dbc, mids)
        self.assertEqual(set(descs), {'glycerol', 'alpha-carotene'})

    # link metanetx.reaction.ecno to uniprot.dbreference.id
    #     where uniprot.dbreference.type = EC
    def test_keggrid2ecno2gene(self, db='Elasticsearch'):
        doctype = "metanetx_reaction"
        dbc = DBconnection(db, self.index)
        keggids = [('R01047', '4.2.1.30', {'dhaB'}),
                   ('R03119', '1.1.1.202', {'dhaT'})
                   ]
        for keggid, ec, genes in keggids:
            if db == "Elasticsearch":
                qc = {"match": {"xrefs.id": keggid}}
                hits, n = qrymtntx.esquery(dbc.es, self.index, qc, doctype, 10)
                ecnos = [r['_source']['ecno'] for r in hits]
            else:
                doctype = "metanetx_reaction"
                qc = {"xrefs.id": keggid}
                hits = dbc.mdbi[doctype].find(qc, limit=10)
                ecnos = [r['ecno'] for r in hits]
            assert len(ecnos) > 0
            for ecno in ecnos:
                print(ecno)
                for ecn in ecno.split(';'):
                    assert ec == ecn
                    r = qryuniprot.getgenes(ecn, db)
                    self.assertSetEqual(genes, set(r))

    def test_keggrid2ecno2gene_mdb(self):
        db = 'MongoDB'
        self.test_keggrid2ecno2gene(db)

    def test_id_queries_es(self):
        self.id_queries("Elasticsearch")

    def test_id_queries_mdb(self):
        self.id_queries("MongoDB")


if __name__ == '__main__':
    unittest.main()

#!/usr/bin/env python
""" Test queries with MetaNetX compounds and reactions """
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

    # First queries metanetx_reactions for given KEGG ids
    # Then links metanetx_reaction.ecno to uniprot.dbReference.id
    #     where uniprot.dbReference.type = EC,  to get gene names
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

    def test_query_reactions(self):
        rids = ["MNXR94726", "MNXR113731"]
        qc = {"_id": {"$in": rids}}
        reacts = qrymtntx.query_reactions(qc)
        assert len(reacts) == len(rids)

    def test_query_metabolites(self):
        mids = ['MNXM39', 'MNXM89612']
        qc = {"_id": {"$in": mids}}
        metabolites = qrymtntx.query_metabolites(qc)
        assert len(metabolites) == len(mids)

    def test_query_compartments(self):
        compartments = qrymtntx.query_compartments()
        assert len(compartments) == 40

    def test_reactionsandmetabolites(self):
        rids = ["MNXR94726", "MNXR113731"]
        qc = {"_id": {"$in": rids}}
        reacts, metabolites = qrymtntx.\
            universalmodel_reactionsandmetabolites(qc)
        assert len(reacts) == len(rids)
        assert len(metabolites) >= len(rids)

    def test_universal_model(self):
        eids = ["1.1.4.13", "2.3.1"]
        qc = {"ecno": {"$in": eids}}
        m = qrymtntx.universal_model(qc)
        assert len(m.reactions) >= len(eids)
        assert len(m.metabolites) >= len(eids)
        import cobra
        tempjson = "test.json"
        cobra.io.save_json_model(m, tempjson, pretty=True)

    def test_universal_model_forlibrary(self):  # execution time ~6s
        qc = {"xrefs.lib": {"$in": ["reactome"]}, "balance": "true"}
        m = qrymtntx.universal_model(qc)
        assert len(m.reactions) >= 900
        assert len(m.metabolites) >= 100
        import cobra
        tempjson = "test2.json"
        cobra.io.save_json_model(m, tempjson, pretty=True)


if __name__ == '__main__':
    unittest.main()

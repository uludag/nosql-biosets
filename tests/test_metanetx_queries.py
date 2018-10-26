#!/usr/bin/env python
""" Test queries with MetaNetX compounds and reactions """
import os
import unittest

from nosqlbiosets.dbutils import DBconnection
from nosqlbiosets.metanetx.query import QueryMetaNetX
from nosqlbiosets.uniprot.query import QueryUniProt

qrymtntx = QueryMetaNetX()
qryuniprot = QueryUniProt("MongoDB", "biosets", "uniprot")


class TestQueryMetanetx(unittest.TestCase):
    d = os.path.dirname(os.path.abspath(__file__))
    index = "biosets"

    def test_compoundnames(self, db="MongoDB"):
        dbc = DBconnection(db, self.index)
        mids = ['MNXM244', 'MNXM39', 'MNXM89612', 'MNXM2000']
        descs = ['3-oxopropanoate', 'formate', 'glycerol', 'alpha-carotene']
        for mid in mids:
            desc = descs.pop(0)
            assert desc == qrymtntx.getcompoundname(dbc, mid)

    def test_id_queries(self, db="MongoDB"):
        keggcids = ['C00222', 'C00116', 'C05433']
        mids = ['MNXM244', 'MNXM89612', 'MNXM2000']
        chebiids = ['17960', '17754', '28425']
        dbc = DBconnection(db, self.index)
        self.assertEqual(mids,
                         qrymtntx.keggcompoundids2otherids(dbc, keggcids))
        self.assertEqual(chebiids,
                         qrymtntx.keggcompoundids2otherids(dbc, keggcids,
                                                           'chebi'))
        self.assertEqual(['cpd00536'],
                         qrymtntx.keggcompoundids2otherids(dbc, ['C00712'],
                                                           'seed'))

    def test_id_queries_es(self):
        self.test_id_queries("Elasticsearch")

    # First queries metanetx_reactions for given KEGG ids
    # Then links metanetx_reaction.ecno to uniprot.dbReference.id
    #     where uniprot.dbReference.type = EC,  to get gene names
    def test_keggrid2ecno2gene(self, db='Elasticsearch'):
        dbc = DBconnection(db, self.index)
        keggids = [('R01047', '4.2.1.30', {'dhaB'}),
                   ('R03119', '1.1.1.202', {'dhaT'})
                   ]
        for keggid, ec, genes in keggids:
            if db == "Elasticsearch":
                qc = {"match": {"xrefs.id": keggid}}
                hits, n = qrymtntx.esquery(dbc.es, "*", qc, '_doc', 10)
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
                    r = qryuniprot.getgenes(ecn)
                    assert all([g in r['primary'] for g in genes])

    def test_keggrid2ecno2gene_mdb(self):
        db = 'MongoDB'
        self.test_keggrid2ecno2gene(db)

    def test_query_reactions(self):
        rids = ["MNXR94726", "MNXR113731"]
        qc = {"_id": {"$in": rids}}
        reacts = qrymtntx.query_reactions(qc)
        assert len(reacts) == len(rids)
        qc = {"xrefs.lib": "kegg"}
        reacts = qrymtntx.query_reactions(qc, projection=['ecno'])
        assert 10262 == len(reacts)

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

    def test_universal_model_tiny(self):
        eids = ["1.1.4.13", "2.3.1"]
        qc = {"ecno": {"$in": eids}}
        m = qrymtntx.universal_model(qc)
        self.assertAlmostEqual(290, len(m.reactions), delta=20)
        self.assertAlmostEqual(560, len(m.metabolites), delta=20)

    def test_universal_model_for_reactome(self):
        qc = {"source.lib": "reactome", "balance": "true"}
        m = qrymtntx.universal_model(qc)
        self.assertAlmostEqual(340, len(m.reactions), delta=20)
        self.assertAlmostEqual(465, len(m.metabolites), delta=20)

    # Find different 'balance' values for reactions referring to KEGG
    def test_reaction_balances(self):
        balance = {"false": 8, "ambiguous": 1070, "true": 7760,
                   "redox": 56, "NA": 1380}
        aggpl = [
            {"$project": {"xrefs": 1, "balance": 1}},
            {"$match": {"xrefs.lib": "kegg"}},
            {"$group": {
                "_id": "$balance",
                "reactions": {"$addToSet": "$xrefs.id"}
            }}
            ]
        r = qrymtntx.dbc.mdbi["metanetx_reaction"].aggregate(aggpl)
        for i in r:
            self.assertAlmostEqual(balance[i['_id']], len(i['reactions']),
                                   delta=10)

    # Similar to above test,
    # only MetaNetX reactions with source.lib == kegg is queried
    def test_kegg_reaction_balances(self):
        balance = {"false": 3, "ambiguous": 340, "true": 685,
                   "redox": 23, "NA": 900}
        aggpl = [
            {"$project": {"source": 1, "balance": 1}},
            {"$match": {"source.lib": "kegg"}},
            {"$group": {
                "_id": "$balance",
                "reactions": {"$addToSet": "$source.id"}
            }}
            ]
        r = qrymtntx.dbc.mdbi["metanetx_reaction"].aggregate(aggpl)
        for i in r:
            self.assertAlmostEqual(balance[i['_id']], len(i['reactions']),
                                   delta=20)


if __name__ == '__main__':
    unittest.main()

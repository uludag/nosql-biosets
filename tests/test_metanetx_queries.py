#!/usr/bin/env python
""" Test queries with MetaNetX compounds and reactions """
import os
import unittest

from nosqlbiosets.dbutils import DBconnection
from nosqlbiosets.graphutils import neighbors_graph, set_degree_as_weight
from nosqlbiosets.graphutils import shortest_paths
from nosqlbiosets.metanetx.query import QueryMetaNetX
from nosqlbiosets.uniprot.query import QueryUniProt

DB = 'biosets'
qrymtntx = QueryMetaNetX(index=DB)
qryuniprot = QueryUniProt("MongoDB", DB, "uniprot-Nov2019")


class TestQueryMetanetx(unittest.TestCase):
    d = os.path.dirname(os.path.abspath(__file__))
    index = "biosets"

    def test_getcompoundname(self, db="MongoDB"):
        qry = QueryMetaNetX(db, self.index)
        mids = ['MNXM244', 'MNXM39', 'MNXM89612', 'MNXM2000']
        descs = ['3-oxopropanoate', 'formate', 'glycerol', 'alpha-carotene']
        for mid in mids:
            desc = descs.pop(0)
            assert desc == qry.getcompoundname(mid)

    def test_getcompoundname_es(self):
        self.test_getcompoundname('Elasticsearch')

    def test_id_queries(self, db="MongoDB"):
        keggcids = ['C00222', 'C00116', 'C05433']
        mids = ['MNXM244', 'MNXM89612', 'MNXM2000']
        chebiids = ['17960', '17754', '28425']
        qry = QueryMetaNetX(db, self.index)
        mids_ = qry.keggcompoundids2otherids(keggcids)
        all(mids[i] in mids_[i] for i in [0, 1, 2])
        chebiids_ = qrymtntx.keggcompoundids2otherids(keggcids, 'chebi')
        all(chebiids[i] in chebiids_[i] for i in [0, 1, 2])
        self.assertEqual(['cpd00536'],
                         qrymtntx.keggcompoundids2otherids(['C00712'], 'seed'))

    def test_id_queries_es(self):
        self.test_id_queries("Elasticsearch")

    # First query MetaNetX_reactions for given KEGG ids
    # Then links MetaNetX_reaction.ecno's to uniprot.dbReference.id
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
            for ecnos_ in ecnos:
                for ecn in ecnos_:
                    assert ec == ecn
                    r = qryuniprot.getgenes(ecn)
                    assert all([g in r['primary'] for g in genes])

    def test_keggrid2ecno2gene_mdb(self):
        db = 'MongoDB'
        self.test_keggrid2ecno2gene(db)

    def test_query_reactions_es(self):
        qry = QueryMetaNetX("Elasticsearch")
        metabolite = 'MNXM556'  # (S)-naringenin
        qc = {"query_string": {"query": metabolite}}
        reacts = qry.query_reactions(qc, size=100)
        self.assertAlmostEqual(15, len(reacts), delta=6)
        for r in reacts:
            r = r['_source']
            for xref in r['xrefs']:
                if xref['lib'] == 'rhea':
                    assert metabolite in r['equation']

    def test_query_reactions(self):
        rids = ["MNXR94726", "MNXR113731"]
        qc = {"_id": {"$in": rids}}
        reacts = qrymtntx.query_reactions(qc)
        assert len(reacts) == len(rids)
        qc = {"xrefs.lib": "kegg"}
        reacts = qrymtntx.query_reactions(qc, projection=['ecno'])
        assert len(reacts) == 10278
        qc = {"source.lib": "bigg", "balance": "true"}
        reacts = qrymtntx.query_reactions(qc, projection=['ecno'])
        assert len(reacts) == 5244
        reacts, metabolites_ = qrymtntx.reactionswithmetabolites(qc)
        assert len(reacts) == 5244

    def test_query_metabolites(self):
        mids = ['MNXM39', 'MNXM89612']
        qc = {"_id": {"$in": mids}}
        metabolites = list(qrymtntx.query_metabolites(qc))
        assert len(metabolites) == len(mids)
        cids = ['C00082']
        qc = {'xrefs.id': cids}
        metabolites = list(qrymtntx.query_metabolites(qc))
        assert 0 == len(metabolites)
        qc = {'xrefs.id': {"$elemMatch": {'$in': cids}}}
        metabolites = list(qrymtntx.query_metabolites(qc))
        assert 1 == len(metabolites)
        qc = {'xrefs.id': {'$in': cids}}
        metabolites = list(qrymtntx.query_metabolites(qc))
        assert 1 == len(metabolites)

    def test_autocomplete_metabolitenames(self):
        names = ['xylitol', '3-oxopropanoate', 'formate', 'squalene',
                 'glycerol', 'D-xylose']
        for name in names:
            for qterm in [name.lower(), name.upper(), name[:4]]:
                r = qrymtntx.autocomplete_metabolitenames(qterm, limit=10)
                assert any(name in i['desc'] for i in r), name

    def test_query_compartments(self):
        compartments = qrymtntx.query_compartments()
        assert len(compartments) == 40

    def test_reactionsandmetabolites(self):
        rids = ["MNXR94726", "MNXR113731"]
        qc = {"_id": {"$in": rids}}
        reacts, metabolites = qrymtntx.\
            reactionswithmetabolites(qc)
        assert len(reacts) == len(rids)
        assert len(metabolites) >= len(rids)

    def test_metabolite_network_tiny(self):
        eids = ["1.1.4.13", "2.3.1"]
        qc = {"ecno": {"$in": eids}}
        m = qrymtntx.get_metabolite_network(qc)
        self.assertAlmostEqual(563, len(m.nodes()), delta=20)
        self.assertAlmostEqual(661, len(m.edges()), delta=20)
        import networkx as nx
        r = nx.single_target_shortest_path_length(m, "Betanin", cutoff=4)
        assert ('celosianin I', 3) in list(r)

    def test_metabolite_network_reactome(self):
        qc = {"source.lib": "reactome", "balance": "true"}
        mn = qrymtntx.get_metabolite_network(qc, max_degree=400)
        self.assertAlmostEqual(430, len(mn.nodes()), delta=60)
        self.assertAlmostEqual(1157, len(mn.edges()), delta=120)
        assert "L-serine" in mn.nodes()
        r = neighbors_graph(mn, "acetyl-CoA", beamwidth=20, maxnodes=200)
        # number of nodes differ based on selected search branches
        self.assertAlmostEqual(143, r.number_of_nodes(), delta=30)
        paths = shortest_paths(mn, 'L-serine', 'acetyl-CoA', 30)
        assert 30 == len(paths)
        assert ['L-serine', 'glycine', 'CoA', 'acetyl-CoA'] in paths[:4]

        set_degree_as_weight(mn)
        paths = shortest_paths(mn, 'L-serine', 'acetyl-CoA', 30,
                               weight='weight')
        assert 30 == len(paths)
        assert ['L-serine', 'glycine', 'CoA', 'acetyl-CoA'] == paths[0]

    def test_metabolite_network_bigg(self):
        qc = {"$or": [{"source.lib": 'bigg'}, {"xrefs.lib": 'bigg'}]}
        mn = qrymtntx.get_metabolite_network(qc, max_degree=1400)
        cmpnds = {'carboxyacetyl-[acyl-carrier protein]', 'acetate',
                  'hexadecanoate', 'xylitol', "L-xylulose", "L-arabinitol"}
        assert len(cmpnds.intersection(mn.nodes())) == 6
        assert mn.degree("L-arabinitol") >= 2
        assert mn.degree("L-xylulose") >= 5
        r = neighbors_graph(mn, "L-arabinitol", beamwidth=5, maxnodes=160)
        assert len(r) == 0  # L-arabinitol is product, not reactant
        paths = shortest_paths(mn, 'alpha-L-arabinan', 'L-arabinitol', 10)
        assert len(paths) == 10
        assert paths[0][1] == 'aldehydo-L-arabinose'
        set_degree_as_weight(mn)
        paths = shortest_paths(mn, 'alpha-L-arabinan', 'L-arabinitol', 10,
                               weight='weight')
        assert len(paths) == 1
        assert paths[0][1] == 'aldehydo-L-arabinose'

        qc = {"source.lib": "bigg", "xrefs.lib": 'bigg', "balance": "true"}
        mn = qrymtntx.get_metabolite_network(qc)
        self.assertAlmostEqual(2815, len(mn.nodes()), delta=200)
        self.assertAlmostEqual(3741, len(mn.edges()), delta=200)
        assert 'L-xylulose' not in mn.nodes()
        assert "Zymosterol ester (yeast specific)" in mn.nodes()
        assert len(cmpnds.intersection(mn.nodes())) == 2
        assert mn.degree("Zymosterol ester (yeast specific)") == 8
        r = neighbors_graph(mn, "L-ascorbate", beamwidth=5, maxnodes=100)
        self.assertAlmostEqual(7, r.number_of_nodes(), delta=4)

    def test_metabolite_network_rhea(self):
        qc = {"source.lib": "rhea", "balance": "true"}
        mn = qrymtntx.get_metabolite_network(qc, max_degree=660)

        paths = shortest_paths(mn, 'L-serine', 'acetyl-CoA', 1600, cutoff=5)
        assert 225 == len(paths)
        assert ['L-serine', 'phosphate', 'acetyl-CoA'] in paths
        set_degree_as_weight(mn)
        paths = shortest_paths(mn, 'L-serine', 'acetyl-CoA', 1600, cutoff=5,
                               weight='weight')
        assert 2 == len(paths)
        assert ['L-serine', '3-(pyrazol-1-yl)-L-alanine', 'O-acetyl-L-serine',
                'acetyl-CoA'] in paths

        self.assertAlmostEqual(7806, len(mn.nodes()), delta=400)
        self.assertAlmostEqual(17698, len(mn.edges()), delta=1200)

        r = neighbors_graph(mn, "L-ascorbate", beamwidth=5, maxnodes=100)
        self.assertAlmostEqual(100, r.number_of_nodes(), delta=12)
        assert 5 == mn.in_degree('L-ascorbate')
        cofactors = ['FMNH2', 'L-ascorbate', 'Cu(+)', 'Cu(2+)', 'Fe(3+)', 'FMN',
                     '5,10-methenyltetrahydrofolate', 'NH4(+)', 'Fe(2+)',
                     'Mn(2+)', "riboflavin", "heme b", "Zn(2+)", "Mg(2+)"]
        for i in cofactors:
            assert i in mn.nodes(), i

    # Find different 'balance' values for reactions referring to KEGG
    def test_reaction_balances(self):
        balance = {"false": 8, "ambiguous": 1065, "true": 7753,
                   "redox": 56, "NA": 1397}
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
        balance = {"false": 3, "ambiguous": 340, "true": 669,
                   "redox": 23, "NA": 871}
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

    def test_source_libraries(self):
        libs = {"seed": 8620, "rhea": 9668, "sabiork": 4774, "reactome": 1309,
                "bigg": 7080, "metacyc": 8837, "kegg": 1893}

        aggpl = [
            {"$project": {"source": 1, "balance": 1}},
            {"$group": {
                "_id": "$source.lib",
                "c": {'$sum': 1}
            }},
            {"$sort": {"c": -1}},
        ]
        r = qrymtntx.dbc.mdbi["metanetx_reaction"].aggregate(aggpl)
        r = {i['_id']: i['c'] for i in r}
        for lib in libs:
            self.assertAlmostEqual(libs[lib], r[lib], delta=30)

    def test_text_search(self):
        libs = {"seed": 7, "sabiork": 93, "reactome": 1,
                "metacyc": 624, "kegg": 229}

        aggpl = [
            {'$match': {'$text': {'$search': 'donor'}}},
            {"$project": {"source": 1, "balance": 1}},
            {"$group": {
                "_id": "$source.lib",
                "c": {'$sum': 1}
            }},
            {"$sort": {"c": -1}},
        ]
        r = qrymtntx.dbc.mdbi["metanetx_reaction"].aggregate(aggpl)
        r = {i['_id']: i['c'] for i in r}
        for lib in libs:
            self.assertAlmostEqual(libs[lib], r[lib], delta=3)


if __name__ == '__main__':
    unittest.main()

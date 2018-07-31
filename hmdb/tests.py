#!/usr/bin/env python
""" Test queries with DrugBank data indexed with MongoDB """
import unittest

from .queries import DOCTYPE
from .queries import QueryDrugBank

# Selected list of antituberculosis drugs, collected from a recent publication
ATdrugs = ['Isoniazid', 'Rifampicin', 'Ethambutol', 'Ethionamide',
           'Pyrazinamide', 'Streptomycin', 'Amikacin', 'Kanamycin',
           'Capreomycin', 'Ciprofloxacin', 'Moxifloxacin', 'Ofloxacin',
           'Cycloserine', 'Aminosalicylic Acid']


class TestQueryDrugBank(unittest.TestCase):
    qry = QueryDrugBank()

    def test_target_genes_interacted_drugs(self):
        genes = ['gyrA', 'katG', 'inhA', 'rpoC', 'rpsL']
        idrugs = ["Chlorzoxazone", "Propyphenazone", "Methylprednisolone",
                  "Esomeprazole", "Propicillin"]
        qc = {'name': {'$in': ATdrugs}}
        r = self.qry.get_target_genes_interacted_drugs(qc, 8000)
        rgenes = {i for _, i, _, _ in r}
        assert all([g in rgenes for g in genes])
        ridrugs = {i for _, _, _, i in r}
        assert all([idr in ridrugs for idr in idrugs])

    def test_distinct_classes(self):
        key = "classification.class"
        names = self.qry.distinctquery(key)
        self.assertIn("Carboxylic Acids and Derivatives", names)
        self.assertAlmostEqual(290, len(names), delta=10)

    def test_distinct_pfam_classes(self):
        key = "transporters.polypeptide.pfams.pfam.name"
        names = self.qry.distinctquery(key)
        self.assertIn("Alpha_kinase", names)
        self.assertAlmostEqual(113, len(names), delta=40)

    def test_distinct_go_classes(self):
        key = "transporters.polypeptide.go-classifiers." \
              "go-classifier.description"
        names = self.qry.distinctquery(key)
        self.assertIn("lipid transport", names)
        self.assertAlmostEqual(1324, len(names), delta=400)

    def test_distinct_drug_targets(self):
        key = "targets.name"
        names = self.qry.distinctquery(key)
        self.assertIn("Isoleucine--tRNA ligase", names)
        self.assertAlmostEqual(4366, len(names), delta=800)
        key = "targets.polypeptide.gene-name"
        names = self.qry.distinctquery(key)
        self.assertIn("RNASE4", names)
        self.assertAlmostEqual(4023, len(names), delta=800)
        key = "targets.polypeptide.source"
        names = self.qry.distinctquery(key)
        self.assertIn("Swiss-Prot", names)
        assert len(names) == 2
        key = "targets.actions.action"
        names = self.qry.distinctquery(key)
        actions = "inhibitor antagonist antibody activator binder intercalation"
        assert set(actions.split(' ')).issubset(names)
        self.assertAlmostEqual(63, len(names), delta=20)

    def test_distinct_atc_codes(self):
        key = "atc-codes.level.#text"
        atc_codes = self.qry.distinctquery(key)
        self.assertIn("Direct thrombin inhibitors", atc_codes)
        self.assertAlmostEqual(950, len(atc_codes), delta=10)
        key = "atc-codes.code"
        atc_codes = self.qry.distinctquery(key)
        self.assertIn("A10AC04", atc_codes)
        self.assertAlmostEqual(3600, len(atc_codes), delta=100)

    def test_query_products(self):
        naprroved = self.qry.aggregate_query([
            {'$unwind': '$products'},
            {"$group": {
                "_id": {'name': '$products.name',
                        'approved': '$products.approved'},
            }},
            {'$group': {'_id': '$_id.approved', 'count': {'$sum': 1}}},
            {"$sort": {"count": -1}},
        ])
        naprroved = list(naprroved)
        self.assertAlmostEqual(69229, naprroved[0]['count'], delta=8000)
        self.assertAlmostEqual(3395, naprroved[1]['count'], delta=800)
        project = {"_id": 1}
        # Drugs with at least one approved product
        qc = {"products.approved": True}
        r = list(self.qry.query(qc, projection=project))
        self.assertAlmostEqual(2800, len(r), delta=100)
        agpl = [
            {'$unwind': '$products'},
            {'$match': qc},
            {'$group': {
                '_id': '$products.name',
                "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {'$limit': 1}
        ]
        r = list(self.qry.aggregate_query(agpl))
        assert 'Ibuprofen' == r[0]['_id']
        self.assertAlmostEqual(780, r[0]['count'], delta=100)
        agpl = [
            {'$project': {
                'products': 1,
                'numberOfProducts': {'$size': "$products"}
            }},
            {'$match': qc},
            {'$group': {
                '_id': None,
                "avg": {"$avg": '$numberOfProducts'},
                "max": {"$max": '$numberOfProducts'},
            }},
        ]
        r = list(self.qry.aggregate_query(agpl))
        self.assertAlmostEqual(11332, r[0]['max'], delta=800)
        agpl = [
            {'$project': {
                'name': 1,
                'products': 1,
                'numberOfProducts': {'$size': "$products"}
            }},
            {'$match': qc},
            {'$sort': {'numberOfProducts': -1}},
            {'$limit': 1},
            {'$project': {'name': 1, 'numberOfProducts': 1}}
        ]
        r = list(self.qry.aggregate_query(agpl))
        assert 'Octinoxate' == r[0]['name']
        qc = {"products.approved": False}
        r = list(self.qry.query(qc, projection=project))
        self.assertAlmostEqual(550, len(r), delta=100)

    def test_query_atc_codes(self):
        project = {"atc-codes": 1}
        qc = {"_id": "DB00001"}
        r = list(self.qry.query(qc, projection=project))
        assert "ANTITHROMBOTIC AGENTS" ==\
               r[0]["atc-codes"][0]["level"][1]["#text"]
        qc = {"_id": "DB00945"}
        r = list(self.qry.query(qc, projection=project))
        assert "B01AC56" in [atc["code"] for atc in r[0]["atc-codes"]]

    def test_kegg_drug_id_to_drugbank_id(self):
        kegg_drugbank_pairs = [("D02361", "DB00777"), ("D00448", "DB00795")]
        for kegg, drugbank in kegg_drugbank_pairs:
            assert drugbank == self.qry.kegg_drug_id_to_drugbank_id(kegg)

    def test_kegg_target_id_to_drugbank_entity_id(self):
        kegg_target_pairs = [
            ("hsa:10", "ARY2_HUMAN", "BE0003607", 'enzymes'),
            ("hsa:100", "ADA_HUMAN", "BE0002214", 'targets'),
            ("hsa:10056", "SYFB_HUMAN", "BE0000561", 'targets')
        ]
        for kegg, uniprotid, entityid, etype in kegg_target_pairs:
            u, r = self.qry.kegg_target_id_to_drugbank_entity_id(kegg,
                                                                 etype=etype)
            assert uniprotid == u
            assert entityid == r

    def test_query_drug_interactions(self):
        project = {"drug-interactions": 1}
        qc = {"_id": "DB00001"}
        r = list(self.qry.query(qc, projection=project))
        assert len(r) == 1
        interactions = r[0]["drug-interactions"]
        assert 'DB01357' in [i["drugbank-id"] for i in interactions]
        assert 'Mestranol' in [i["name"] for i in interactions]
        key = "drug-interactions.name"
        names = self.qry.distinctquery(key)
        self.assertIn("Calcium Acetate", names)
        self.assertAlmostEqual(3300, len(names), delta=100)

        key = "drug-interactions.drugbank-id"
        idids = self.qry.distinctquery(key)
        self.assertIn("DB00048", idids)
        self.assertAlmostEqual(3300, len(idids), delta=100)
        # list of approved drugs that interacts with at least one other drug
        dids = self.qry.distinctquery('_id',
                                      qc={"products.approved": True,
                                          "drug-interactions":
                                              {"$not": {"$size": 0}}})
        self.assertIn("DB00048", dids)
        self.assertAlmostEqual(2810, len(dids), delta=100)

    def test_drug_interactions_graph(self):
        qc = {"affected-organisms": {
            "$in": ["Hepatitis B virus"]}}
        g = self.qry.get_connections_graph(qc, "drug-interactions")
        self.assertAlmostEqual(440, g.number_of_edges(), delta=10)
        self.assertAlmostEqual(430, g.number_of_nodes(), delta=10)
        assert 'Pregnenolone' in g.nodes
        assert g.number_of_selfloops() == 0

    def test_drug_targets_graph(self):
        from os import unlink
        qc = {'$text': {'$search': 'defensin'}}
        gfile = "defensin-targets.xml"
        g = self.qry.get_connections_graph(qc, "targets", gfile)
        assert g.number_of_edges() == 68
        assert g.number_of_nodes() == 62
        unlink(gfile)
        gfile = "defensin-enzymes.d3js.json"
        g = self.qry.get_connections_graph(qc, "enzymes", gfile)
        assert g.number_of_edges() == 1
        assert g.number_of_nodes() == 2
        unlink(gfile)
        gfile = "defensin-transporters.json"
        g = self.qry.get_connections_graph(qc, "transporters", gfile)
        assert g.number_of_edges() == 0
        assert g.number_of_nodes() == 0
        unlink(gfile)
        gfile = "defensin-carriers.gml"
        g = self.qry.get_connections_graph(qc, "carriers", gfile)
        assert g.number_of_edges() == 0
        assert g.number_of_nodes() == 0
        unlink(gfile)

    def test_drug_targets_graph_merge(self):
        import networkx as nx
        qc = {'$text': {'$search': 'lipid'}}
        g1 = self.qry.get_connections_graph(qc, "targets")
        self.assertAlmostEqual(3669, g1.number_of_edges(), delta=800)
        self.assertAlmostEqual(2530, g1.number_of_nodes(), delta=800)
        g2 = self.qry.get_connections_graph(qc, "enzymes")
        self.assertAlmostEqual(808, g2.number_of_edges(), delta=200)
        self.assertAlmostEqual(290, g2.number_of_nodes(), delta=100)
        g3 = self.qry.get_connections_graph(qc, "transporters")
        self.assertAlmostEqual(540, g3.number_of_edges(), delta=100)
        self.assertAlmostEqual(244, g3.number_of_nodes(), delta=80)
        g4 = self.qry.get_connections_graph(qc, "carriers")
        self.assertAlmostEqual(144, g4.number_of_edges(), delta=40)
        self.assertAlmostEqual(121, g4.number_of_nodes(), delta=40)
        g = nx.compose_all([g1, g2, g3, g4])
        self.assertAlmostEqual(5173, g.number_of_edges(), delta=800)
        self.assertAlmostEqual(2715, g.number_of_nodes(), delta=400)

    def test_get_allnetworks(self):
        tests = [
            # ({}, 22927, 11376),
            ({"name": "Acetaminophen"}, 26, 27),
            ({'$text': {'$search': 'lipid'}}, 5094, 2715)
        ]
        for qc, nedges, nnodes in tests:
            g = self.qry.get_allnetworks(qc)
            self.assertAlmostEqual(nedges, g.number_of_edges(), delta=800)
            self.assertAlmostEqual(nnodes, g.number_of_nodes(), delta=800)

    def test_drug_enzymes_graph(self):
        qc = {"affected-organisms": {
            "$in": ["Hepatitis B virus"]}}
        g = self.qry.get_connections_graph(qc, "enzymes")
        assert g.number_of_edges() == 13
        assert g.number_of_nodes() == 14
        assert g.number_of_selfloops() == 0

    def test_target_genes(self):
        agpl = [
            {'$match': {'$text': {'$search': 'defensin'}}},
            {'$project': {
                'targets.polypeptide': 1}},
            {'$unwind': '$targets'},
            {'$group': {
                '_id': '$targets.polypeptide.gene-name', "count": {"$sum": 1}}}
        ]
        r = self.qry.aggregate_query(agpl)
        genes = [c['_id'] for c in r]
        assert len(genes) == 43
        self.assertIn('PRSS3', genes)

    def test_number_of_interacted_drugs(self):
        tests = [
            ([(0, 40), (1, 175), (2, 983)],
             {'$text': {'$search': "antitubercular"}}),
            ([(0, 6), (1, 23), (2, 137)],
             {"name": "Ribavirin"})
        ]
        for test, qc in tests:
            for maxdepth, nr in test:
                agpl = [
                    {'$match': qc},
                    {"$graphLookup": {
                        "from": DOCTYPE,
                        "startWith": "$name",
                        "connectToField":
                            "drug-interactions.name",
                        "connectFromField":
                            "name",
                        "as": "neighbors",
                        "maxDepth": maxdepth,
                        "depthField": "depth",
                        "restrictSearchWithMatch": {
                            "classification.class":
                                "Carboxylic Acids and Derivatives"
                        }
                    }},
                    {"$unwind": "$neighbors"},
                    {'$project': {
                        'name': 1,
                        'neighbors.name': 1
                    }}
                ]
                r = self.qry.aggregate_query(agpl)
                r = [c for c in r]
                self.assertAlmostEqual(nr, len(r), delta=20)


if __name__ == '__main__':
    unittest.main()

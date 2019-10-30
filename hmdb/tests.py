#!/usr/bin/env python
""" Test queries with DrugBank data indexed with MongoDB """
import os
import unittest

import networkx as nx
from nosqlbiosets.graphutils import neighbors_graph
from nosqlbiosets.graphutils import remove_highly_connected_nodes
from nosqlbiosets.graphutils import remove_small_subgraphs
from nosqlbiosets.graphutils import save_graph

from .queries import DOCTYPE
from .queries import QueryDrugBank

# Selected list of antituberculosis drugs, collected from a KAUST publication
ATdrugs = ['Isoniazid', 'Rifampicin', 'Ethambutol', 'Ethionamide',
           'Pyrazinamide', 'Streptomycin', 'Amikacin', 'Kanamycin',
           'Capreomycin', 'Ciprofloxacin', 'Moxifloxacin', 'Ofloxacin',
           'Cycloserine', 'Aminosalicylic Acid']

EX_GRAPHS = os.path.dirname(os.path.abspath(__file__)) + \
           '/../docs/example-graphs/'


class TestQueryDrugBank(unittest.TestCase):
    qry = QueryDrugBank(index='biosets', host='localhost')

    def test_target_genes_interacted_drugs(self):
        genes = ['gyrA', 'katG', 'inhA', 'rpoC', 'rpsL']
        idrugs = ["Chlorzoxazone", "Propyphenazone", "Methylprednisolone",
                  "Esomeprazole", "Propicillin"]
        qc = {'name': {'$in': ATdrugs}}
        r = self.qry.get_target_genes_interacted_drugs(qc, 18000)
        rgenes = {i for _, i, _, _ in r}
        assert all([g in rgenes for g in genes])
        ridrugs = {i for _, _, _, i in r}
        assert all([idr in ridrugs for idr in idrugs])

    def test_autocomplete_drugnames(self):
        pairs = [  # (query-term, expected-name)
            ("Gefitinib", "Gefitinib"), ("Wortmannin", "Wortmannin"),
            ("advil", "Phenylephrine"), ("Reopro", "Abciximab"),
            ("Eliquis", "Apixaban"), ("7-select Advil PM", "Ibuprofen")
        ]
        for qterm, name in pairs:
            for qterm_ in [qterm.lower(), qterm.upper(), qterm[:4]]:
                r = self.qry.autocomplete_drugnames(qterm_)
                assert any(name in i['name'] for i in r), name

        for name in ATdrugs:
            for qterm in [name.lower(), name.upper(), name[:4]]:
                r = self.qry.autocomplete_drugnames(qterm)
                assert any(name in i['name'] for i in r), name

    def test_distinct_classes(self):
        key = "classification.class"
        names = self.qry.distinctquery(key)
        self.assertIn("Carboxylic Acids and Derivatives", names)
        self.assertAlmostEqual(302, len(names), delta=10)

    def test_distinct_groups(self):
        key = "groups"
        names = self.qry.distinctquery(key)
        self.assertIn("withdrawn", names)
        self.assertIn("investigational", names)
        self.assertAlmostEqual(7, len(names), delta=0)

    def test_distinct_pfam_classes(self):
        key = "transporters.polypeptide.pfams.pfam.name"
        names = self.qry.distinctquery(key)
        self.assertIn("Alpha_kinase", names)
        self.assertAlmostEqual(113, len(names), delta=40)

    def test_distinct_go_classes(self):
        key = "transporters.polypeptide.go-classifiers.description"
        names = self.qry.distinctquery(key)
        self.assertIn("lipid transport", names)
        self.assertAlmostEqual(1324, len(names), delta=400)

    def test_distinct_targets_features(self):
        # Drugs that have at least one target: ~7400
        agpl = [
            {'$match': {'targets': {'$exists': True}
                        }},
            {'$group': {
                '_id': None,
                "count": {"$sum": 1}
            }}]
        r = list(self.qry.aggregate_query(agpl))
        self.assertAlmostEqual(7500, r[0]['count'], delta=60)

        # Number of unique target names: ~4360
        names = self.qry.distinctquery("targets.name")
        self.assertIn("Isoleucine--tRNA ligase", names)
        self.assertAlmostEqual(4366, len(names), delta=60)

        # Number of unique target ids: ~4860
        ids = self.qry.distinctquery("targets.id")
        self.assertIn("BE0000198", ids)
        self.assertAlmostEqual(4865, len(ids), delta=60)

        names = self.qry.distinctquery("targets.polypeptide.gene-name")
        self.assertIn("RNASE4", names)
        self.assertAlmostEqual(4023, len(names), delta=60)

        names = self.qry.distinctquery("targets.polypeptide.source")
        self.assertSetEqual({'TrEMBL', "Swiss-Prot"}, set(names))

        names = self.qry.distinctquery("targets.actions.action")
        actions = "inhibitor antagonist antibody activator binder intercalation"
        assert set(actions.split(' ')).issubset(names)
        self.assertAlmostEqual(63, len(names), delta=4)

    def test_distinct_atc_codes(self):
        key = "atc-codes.level.#text"
        atc_codes = self.qry.distinctquery(key)
        self.assertIn("Direct thrombin inhibitors", atc_codes)
        self.assertAlmostEqual(981, len(atc_codes), delta=10)
        key = "atc-codes.code"
        atc_codes = self.qry.distinctquery(key)
        self.assertIn("A10AC04", atc_codes)
        self.assertAlmostEqual(4478, len(atc_codes), delta=140)

    def test_distinct_ahfs_codes(self):
        key = "ahfs-codes.ahfs-code"
        ahfs_codes = self.qry.distinctquery(key)
        self.assertIn("72:00.00", ahfs_codes)
        self.assertAlmostEqual(347, len(ahfs_codes), delta=10)

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
        self.assertAlmostEqual(44693, naprroved[0]['count'], delta=8000)
        self.assertAlmostEqual(36078, naprroved[1]['count'], delta=800)
        project = {"_id": 1}
        # Drugs with at least one approved product
        qc = {"products.approved": True}
        r = list(self.qry.query(qc, projection=project))
        self.assertAlmostEqual(3220, len(r), delta=100)
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
        assert 'Hydrocodone Bitartrate and Acetaminophen' == r[0]['_id']
        self.assertAlmostEqual(1063, r[0]['count'], delta=100)
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
        self.assertAlmostEqual(12751, r[0]['max'], delta=800)
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
        self.assertAlmostEqual(1282, len(r), delta=100)

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

    def test_kegg_target_id_to_drugbank_id(self):
        kegg_target_pairs = [
            ("hsa:10", "ARY2_HUMAN", "BE0003607", 'enzymes'),
            ("hsa:100", "ADA_HUMAN", "BE0002214", 'targets'),
            ("hsa:10056", "SYFB_HUMAN", "BE0000561", 'targets')
        ]
        for kegg, uniprotid, entityid, etype in kegg_target_pairs:
            u, r = self.qry.kegg_target_id_to_drugbank_entity_id(
                kegg, etype=etype, uniprotcollection="uniprot-Saf1441")
            assert uniprotid == u
            assert entityid == r

    def test_query_drug_interactions(self):
        project = {"drug-interactions": 1}
        qc = {"_id": "DB00001"}
        r = list(self.qry.query(qc, projection=project))
        assert 1 == len(r)
        interactions = r[0]["drug-interactions"]
        assert 647 == len(interactions)

        qc = {"_id": "DB00072"}
        r = list(self.qry.query(qc, projection=project))
        assert 1 == len(r)
        interactions = r[0]["drug-interactions"]
        assert 578 == len(interactions)
        assert 'DB08879' in [i["drugbank-id"] for i in interactions]
        names = [i["name"] for i in interactions]
        assert 'Begelomab' in names

        key = "drug-interactions.name"
        names = self.qry.distinctquery(key)
        self.assertIn("Salmeterol", names)
        self.assertAlmostEqual(3951, len(names), delta=200)

        key = "drug-interactions.drugbank-id"
        idids = self.qry.distinctquery(key)
        self.assertIn("DB00048", idids)
        self.assertAlmostEqual(3950, len(idids), delta=160)
        # list of approved drugs that interacts with at least one other drug
        dids = self.qry.distinctquery('_id',
                                      qc={"products.approved": True,
                                          "drug-interactions":
                                              {"$not": {"$size": 0}}})
        self.assertIn("DB00048", dids)
        self.assertAlmostEqual(3220, len(dids), delta=100)

    def test_drug_interactions_graph(self):
        qc = {"affected-organisms": {
            "$in": ["Hepatitis B virus"]}}
        g = self.qry.get_connections_graph(qc, "drug-interactions")
        self.assertAlmostEqual(5868, g.number_of_edges(), delta=100)
        self.assertAlmostEqual(1609, g.number_of_nodes(), delta=100)
        assert 'Metampicillin' in g.nodes
        assert g.number_of_selfloops() == 0

    def test_example_text_queries(self):
        for qterm, n in [
            ('coronavirus', 5), ('MERS-CoV', 6), ('mrsa', 12),
            ('methicillin', 23), ('meticillin', 1), ('defensin', 15)
        ]:
            qc = {'$text': {'$search': qterm}}
            g = self.qry.query(qc, projection={"_id": 1})
            assert len(list(g)) == n, qterm

    def test_example_graph(self):
        qc = {'$text': {'$search': 'methicillin'}}
        g1 = self.qry.get_connections_graph(qc, "targets")
        self.assertAlmostEqual(82, g1.number_of_edges(), delta=10)
        self.assertAlmostEqual(73, g1.number_of_nodes(), delta=10)
        g2 = self.qry.get_connections_graph(qc, "enzymes")
        self.assertAlmostEqual(15, g2.number_of_edges(), delta=2)
        self.assertAlmostEqual(14, g2.number_of_nodes(), delta=1)
        g3 = self.qry.get_connections_graph(qc, "transporters")
        self.assertAlmostEqual(22, g3.number_of_edges(), delta=4)
        self.assertAlmostEqual(22, g3.number_of_nodes(), delta=4)
        g4 = self.qry.get_connections_graph(qc, "carriers")
        self.assertAlmostEqual(7, g4.number_of_edges(), delta=4)
        self.assertAlmostEqual(8, g4.number_of_nodes(), delta=4)
        r = nx.compose_all([g1, g2, g3, g4])
        self.assertAlmostEqual(125, r.number_of_edges(), delta=20)
        self.assertAlmostEqual(94, r.number_of_nodes(), delta=14)
        remove_small_subgraphs(r)
        save_graph(r, EX_GRAPHS + 'drugbank-methicillin.json')
        r = neighbors_graph(r, "Ticarcillin", beamwidth=8, maxnodes=100)
        assert 2 == r.number_of_nodes()

    def test_drug_targets_graph(self):
        qc = {'$text': {'$search': '\"side effects\"'}}
        g = self.qry.get_connections_graph(qc, "targets")
        assert g.number_of_edges() == 535
        assert g.number_of_nodes() == 311
        assert "Olanzapine" in g.nodes
        assert "CCX915" in g.nodes
        r = neighbors_graph(g, "Olanzapine")
        assert 5 == r.number_of_nodes()

        qc = {'$text': {'$search': 'defensin'}}
        gfile = "./docs/example-graphs/defensin-targets.json"
        g = self.qry.get_connections_graph(qc, "targets", gfile)
        assert g.number_of_edges() == 68
        assert g.number_of_nodes() == 62
        g = self.qry.get_connections_graph(qc, "enzymes")
        assert g.number_of_edges() == 1
        assert g.number_of_nodes() == 2
        g = self.qry.get_connections_graph(qc, "transporters")
        assert g.number_of_edges() == 0
        assert g.number_of_nodes() == 0
        g = self.qry.get_connections_graph(qc, "carriers")
        assert g.number_of_edges() == 0
        assert g.number_of_nodes() == 0

    def test_drug_targets_graph_merge(self):
        qc = {'$text': {'$search': 'lipid'}}
        g1 = self.qry.get_connections_graph(qc, "targets")
        self.assertAlmostEqual(4000, g1.number_of_edges(), delta=80)
        self.assertAlmostEqual(2530, g1.number_of_nodes(), delta=80)
        target = "Lys-63-specific deubiquitinase BRCC36"
        assert target in g1.nodes
        assert "targets" == g1.nodes[target]['type']
        g2 = self.qry.get_connections_graph(qc, "enzymes")
        self.assertAlmostEqual(842, g2.number_of_edges(), delta=30)
        self.assertAlmostEqual(349, g2.number_of_nodes(), delta=20)
        g3 = self.qry.get_connections_graph(qc, "transporters")
        self.assertAlmostEqual(677, g3.number_of_edges(), delta=20)
        self.assertAlmostEqual(260, g3.number_of_nodes(), delta=30)
        g4 = self.qry.get_connections_graph(qc, "carriers")
        self.assertAlmostEqual(144, g4.number_of_edges(), delta=40)
        self.assertAlmostEqual(121, g4.number_of_nodes(), delta=40)
        g = nx.compose_all([g1, g2, g3, g4])
        assert target in g.nodes
        assert "targets" == g1.nodes[target]['type']
        self.assertAlmostEqual(5578, g.number_of_edges(), delta=80)
        self.assertAlmostEqual(2761, g.number_of_nodes(), delta=80)
        remove_highly_connected_nodes(g)
        self.assertAlmostEqual(768, g.number_of_edges(), delta=80)
        self.assertAlmostEqual(2525, g.number_of_nodes(), delta=40)
        remove_small_subgraphs(g, 20)
        self.assertAlmostEqual(461, g.number_of_edges(), delta=80)
        self.assertAlmostEqual(2093, g.number_of_nodes(), delta=40)

    def test_get_allgraphs(self):
        tests = [
            # ({}, 25624, 12175),
            ({"name": "Acetaminophen"}, 26, 27),
            ({'$text': {'$search': 'lipid'}}, 5637, 2751)
        ]
        for qc, nedges, nnodes in tests:
            g = self.qry.get_allgraphs(qc)
            self.assertAlmostEqual(nedges, g.number_of_edges(), delta=10)
            self.assertAlmostEqual(nnodes, g.number_of_nodes(), delta=10)

    def test_drug_enzymes_graph(self):
        qc = {"affected-organisms": {
            "$in": ["Hepatitis B virus"]}}
        g = self.qry.get_connections_graph(qc, "enzymes")
        assert g.number_of_edges() == 25
        assert g.number_of_nodes() == 20
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
            ([(0, 264), (1, 817)],
             {'$text': {'$search': "\"treatment of Tuberculosis\""}}),
            ([(0, 35), (1, 149), (2, 187)],
             {'$text': {'$search': "\"antitubercular agent\""}}),
            ([(0, 13), (1, 43), (2, 50)],
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
                                "Diazines"
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
                self.assertAlmostEqual(nr, len(r), delta=nr/6)

    def test_number_of_target_connected_drugs(self):
        """"Number of drugs which are neighbors through shared targets"""
        tests = [
            ([(0, 0), (1, 0)],
             {'$text': {'$search': "antitubercular"}}),
            ([(0, 457), (1, 579)],
             {'$text': {'$search': "tuberculosis"}}),
            ([(0, 365), (1, 365), (2, 365)],
             {"name": "Ribavirin"})
        ]
        for test, qc in tests:
            for maxdepth, nr in test:
                agpl = [
                    {'$match': qc},
                    {'$unwind': '$targets'},
                    {'$unwind': '$targets.polypeptide'},
                    {"$graphLookup": {
                        "from": DOCTYPE,
                        "startWith": "$targets.polypeptide.gene-name",
                        "connectToField":
                            "targets.polypeptide.gene-name",
                        "connectFromField":
                            "targets.polypeptide.gene-name",
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
                r = self.qry.aggregate_query(agpl, allowDiskUse=True)
                r = [c for c in r]
                assert len(r) == nr, maxdepth


if __name__ == '__main__':
    unittest.main()

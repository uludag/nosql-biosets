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
        assert 292 == len(names)

    def test_distinct_pfam_classes(self):
        key = "transporters.polypeptide.pfams.pfam.name"
        names = self.qry.distinctquery(key)
        self.assertIn("Alpha_kinase", names)
        assert 90 == len(names)

    def test_distinct_go_classes(self):
        key = "transporters.polypeptide.go-classifiers." \
              "go-classifier.description"
        names = self.qry.distinctquery(key)
        self.assertIn("lipid transport", names)
        assert 1148 == len(names)

    def test_distinct_drug_targets(self):
        key = "targets.name"
        names = self.qry.distinctquery(key)
        self.assertIn("Isoleucine--tRNA ligase", names)
        assert 4022 == len(names)
        key = "targets.polypeptide.gene-name"
        names = self.qry.distinctquery(key)
        self.assertIn("RNASE4", names)
        assert 3757 == len(names)
        key = "targets.polypeptide.source"
        names = self.qry.distinctquery(key)
        self.assertIn("Swiss-Prot", names)
        assert len(names) == 2
        key = "targets.actions.action"
        names = self.qry.distinctquery(key)
        actions = "inhibitor antagonist antibody activator binder intercalation"
        assert set(actions.split(' ')).issubset(names)
        assert len(names) == 49

    def test_distinct_atc_codes(self):
        key = "atc-codes.level.#text"
        atc_codes = self.qry.distinctquery(key)
        self.assertIn("Direct thrombin inhibitors", atc_codes)
        assert 939 == len(atc_codes)
        key = "atc-codes.code"
        atc_codes = self.qry.distinctquery(key)
        self.assertIn("A10AC04", atc_codes)
        assert 3496 == len(atc_codes)

    def test_query_products(self):
        names = self.qry.aggregate_query([
            {'$unwind': '$products'},
            {"$group": {
                "_id": {'name': '$products.name',
                        'approved': '$products.approved'},
            }},
            {'$group': {'_id': '$_id.approved', 'count': {'$sum': 1}}},
            {"$sort": {"count": -1}},
        ])
        names = list(names)
        assert 65054 == names[0]['count']
        assert 3843 == names[1]['count']
        project = {"_id": 1}
        # Drugs with at least one approved product
        qc = {"products.approved": True}
        r = list(self.qry.query(qc, projection=project))
        assert 2695 == len(r)
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
        assert {'_id': 'Ibuprofen', 'count': 708} == r[0]
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
        assert 10755 == r[0]['max']
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
        assert 708 == len(r)

    def test_query_atc_codes(self):
        project = {"atc-codes": 1}
        qc = {"_id": "DB00001"}
        r = list(self.qry.query(qc, projection=project))
        expected = "ANTITHROMBOTIC AGENTS"
        assert r[0]["atc-codes"][0]["level"][1]["#text"] == expected
        qc = {"_id": "DB00945"}
        r = list(self.qry.query(qc, projection=project))
        expected = "C10BX04"
        assert r[0]["atc-codes"][0]["code"] == expected

    def test_query_kegg_drug_ids(self):
        kegg_drugbank_pairs = [("D02361", "DB00777"), ("D00448", "DB00795")]
        for kegg, drugbank in kegg_drugbank_pairs:
            project = {"external-identifiers": 1}
            qc = {"external-identifiers.identifier": kegg}
            r = list(self.qry.query(qc, projection=project))
            assert drugbank == r[0]["_id"]

    def test_query_kegg_targets(self):
        from nosqlbiosets.uniprot.query import QueryUniProt
        qryuniprot = QueryUniProt("MongoDB", "biosets", "uniprot")
        kegg_target_pairs = [("hsa:10056", "SYFB_HUMAN", "BE0000692")]
        for kegg, uniprot, target in kegg_target_pairs:
            g = qryuniprot.getnamesforkegg_geneids([kegg])
            project = {"targets": 1}
            qc = {"targets.polypeptide.external-identifiers."
                  "external-identifier.identifier": g[0]}
            r = list(self.qry.query(qc, projection=project))
            assert target == r[0]["targets"][0]["id"]

    def test_query_drug_interactions(self):
        project = {"drug-interactions": 1}
        qc = {"_id": "DB00001"}
        r = list(self.qry.query(qc, projection=project))
        assert len(r) == 1
        interactions = r[0]["drug-interactions"]
        assert interactions[1]["drugbank-id"] == 'DB00346'
        assert interactions[1]["name"] == 'Alfuzosin'
        key = "drug-interactions.name"
        names = self.qry.distinctquery(key)
        self.assertIn("Calcium Acetate", names)
        assert 3138 == len(names)

        key = "drug-interactions.drugbank-id"
        idids = self.qry.distinctquery(key)
        self.assertIn("DB00048", idids)
        assert 3138 == len(idids)
        # list of approved drugs that interacts with at least one other drug
        dids = self.qry.distinctquery('_id',
                                      qc={"products.approved": True,
                                          "drug-interactions":
                                              {"$not": {"$size": 0}}})
        self.assertIn("DB00048", dids)
        assert 2695 == len(dids)

    def test_drug_interactions_graph(self):
        qc = {"affected-organisms": {
            "$in": ["Hepatitis B virus"]}}
        g = self.qry.get_connections_graph(qc, "drug-interactions")
        assert 435 == g.number_of_edges()
        assert 428 == g.number_of_nodes()
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
        assert 2842 == g1.number_of_edges()
        assert 2061 == g1.number_of_nodes()
        g2 = self.qry.get_connections_graph(qc, "enzymes")
        assert 594 == g2.number_of_edges()
        assert 260 == g2.number_of_nodes()
        g3 = self.qry.get_connections_graph(qc, "transporters")
        assert 463 == g3.number_of_edges()
        assert 213 == g3.number_of_nodes()
        g4 = self.qry.get_connections_graph(qc, "carriers")
        assert 101 == g4.number_of_edges()
        assert 93 == g4.number_of_nodes()
        g = nx.compose_all([g1, g2, g3, g4])
        assert 3977 == g.number_of_edges()
        assert 2220 == g.number_of_nodes()

    def test_get_allnetworks(self):
        tests = [
            # ({}, 22927, 11376),
            ({"name": "Acetaminophen"}, 26, 27),
            ({'$text': {'$search': 'lipid'}}, 3977, 2220)
        ]
        for qc, nedges, nnodes in tests:
            g = self.qry.get_allnetworks(qc)
            assert nedges == g.number_of_edges()
            assert nnodes == g.number_of_nodes()

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
            ([(0, 38), (1, 108), (2, 690)],
             {'$text': {'$search': "antitubercular"}}),
            ([(0, 6), (1, 11), (2, 93)],
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
                assert nr == len(r)


if __name__ == '__main__':
    unittest.main()

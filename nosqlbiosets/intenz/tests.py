#!/usr/bin/env python
""" Test queries with IntEnz data indexed with MongoDB and Neo4j"""

import unittest

from nosqlbiosets.dbutils import DBconnection
from .query import QueryIntEnz

qryintenz = QueryIntEnz()


class TestQueryIntEnz(unittest.TestCase):

    def test_getreactant_product_names(self):
        re = qryintenz.getreactantnames()
        self.assertAlmostEqual(4200, len(re), delta=100,
                               msg="Number of reactant names")
        pr = qryintenz.getproductnames()
        self.assertAlmostEqual(4800, len(pr), delta=100,
                               msg="Number of product names")
        i = set(re).intersection(pr)
        self.assertAlmostEqual(2030, len(i), delta=40,
                               msg="Chemicals both in reactants and products")
        assert "cytidine" in i

    def test_enzymeswithreactant(self):
        tests = [
            # reactant, name of one expected enzyme, # of expected enzymes
            ("2-oxoglutarate", "Thymine dioxygenase", 130),
            ("L-glutamate", "Glutamate--tRNA(Gln) ligase", 37),
            ("3-oxopropanoate", "Malonate-semialdehyde dehydrogenase", 5),
            ("glycerol", "D/L-glyceraldehyde reductase", 8),
            ("D-threo-isocitrate", "Isocitrate--homoisocitrate dehydrogenase",
             3)
        ]
        for r, e, n in tests:
            enzymes = qryintenz.getenzymeswithreactant(r)
            assert e in [e[1] for e in enzymes]
            self.assertAlmostEqual(n, len(enzymes), delta=3)

    def test_enzymeswithreactant_chebiid(self):
        tests = [
            # ChEBI id, id and name of one enzyme, # of enzymes
            (32682, "3.5.3.6", "Arginine deiminase", 22),
            (58098, "4.1.2.20", "2-dehydro-3-deoxyglucarate aldolase", 1)
        ]
        for chebiid, ecn, ename, n in tests:
            enzymes = qryintenz.getenzymeswithreactant_chebiid(chebiid)
            assert (ecn, ename) in enzymes
            assert n == len(enzymes)

    def test_enzymeswithproduct_chebiid(self):
        tests = [
            # ChEBI id, id and name of one enzyme, # of enzymes
            (32682, "3.6.3.21", "Polar-amino-acid-transporting ATPase", 5),
            (15361, "4.1.2.20", "2-dehydro-3-deoxyglucarate aldolase", 100),
            (33019, "2.7.7.19", "Polynucleotide adenylyltransferase", 471)
        ]
        for chebiid, ecn, ename, n in tests:
            enzymes = qryintenz.getenzymeswithproduct_chebiid(chebiid)
            assert (ecn, ename) in enzymes
            assert n == len(enzymes)

    def test_query_products(self):
        rnames = {
            "2-oxoglutarate": ["Isocitrate--homoisocitrate dehydrogenase"]
        }
        for name in rnames:
            enzymes = qryintenz.query_products([name])
            assert rnames[name][0] in [
                e["accepted_name"]["#text"] for e in enzymes]

    def test_enzyme_names(self):
        enzyms = [("2.7.7.19", "Polynucleotide adenylyltransferase"),
                  ("4.2.1.30", "Glycerol dehydratase"),
                  ("1.1.1.202", "1,3-propanediol dehydrogenase")]

        eids = qryintenz.getenzymeswithids([eid for eid, _ in enzyms])
        assert len(eids) == 3
        for enzym in eids:
            assert enzym in enzyms

        for eid, enz in enzyms:
            eids = qryintenz.enzyme_name2id([enz])
            assert len(eids) == 1 and eids[0] == eid
            e = qryintenz.getenzymebyid(eid)
            assert e is not None and e["accepted_name"]["#text"] == enz
            eids = qryintenz.getenzymeswithids([eid])
            assert len(eids) == 1 and eids[0][1] == enz

    def test_query_reactantandproduct(self):
        ste = [
            ("isocitrate", "glyoxylate", "Isocitrate lyase"),
            ("pyruvate", "L-alanine", "Alanine dehydrogenase"),
            ("2-oxoglutarate", "D-threo-isocitrate",
             "Isocitrate--homoisocitrate dehydrogenase"),
            ("2-oxoglutarate", "glyoxylate",
             "Glycine transaminase")
        ]
        for source, target, enz in ste:
            r = qryintenz.query_reactantandproduct(source, target)
            assert len(r) >= 1
            assert enz in r

    def test_getconnections(self):
        tests = [
            # Isocitrate lyase
            ("isocitrate", "glyoxylate",
             # "isocitrate <?> glyoxylate + succinate",
             "4.1.3.1"),
            # "Isocitrate--homoisocitrate dehydrogenase"
            ("D-threo-isocitrate", "2-oxoglutarate",
             # "D-threo-isocitrate + NAD(+) <?> 2-oxoglutarate + CO2 + NADH",
             "1.1.1.286")
        ]
        r = qryintenz.get_connections({})
        self.assertAlmostEqual(38165, len(r), delta=100)
        r = {(e['reactant'], e['product'], e['enzyme']) for e in r}
        for c in tests:
            assert c in r

    def test_getconnections_graph(self):
        qc = {'reactions.label.value': "Chemically balanced"}
        g = qryintenz.get_connections_graph(qc, limit=40000)
        self.assertAlmostEqual(38160, g.number_of_edges(), delta=200)
        self.assertAlmostEqual(7030,  g.number_of_nodes(), delta=200)

    def test_lookup_connected_metabolites(self):
        tests = [
            ("2-dehydro-3-deoxy-D-glucarate", "2-methyl-3-oxopropanoate",
             "2-dehydro-3-deoxyglucarate aldolase",
             '4.1.2.20',
             "(R)-3-amino-2-methylpropionate--pyruvate transaminase",
             '2.6.1.40'),
            ("pyruvate", "beta-alanine",
             "(R)-3-amino-2-methylpropionate--pyruvate transaminase",
             '2.6.1.40',
             "Beta-alanine--pyruvate transaminase", '2.6.1.18'),
            ("2-oxoglutarate", "glyoxylate",
             "LL-diaminopimelate aminotransferase", "2.6.1.83",
             "Glycine oxidase", "1.4.3.19"),
            ("2-oxoglutarate", "glyoxylate",
             "LL-diaminopimelate aminotransferase", "2.6.1.83",
             "Glycine--oxaloacetate transaminase", "2.6.1.35")
        ]

        def check(nextenz):
            assert e1, ecn1 in [
                (e["accepted_name"]["#text"], e["_id"]) for e in r]
            assert ecn2 in [e[nextenz]["_id"] for e in r]
            assert e2 in [e[nextenz]["accepted_name"]["#text"] for e in r]

        for source, target, e1, ecn1, e2, ecn2 in tests:
            r = qryintenz.lookup_connected_metabolites(source, target)
            check("enzyme2")
            r = qryintenz.graphlookup_connected_metabolites(source, target, 0)
            check("enzymes")
            break  # let usual tests return faster

    def test_neo4j_graphsearch_connected_metabolites(self):
        tests = [("2-oxoglutarate", "glyoxylate", 1)]
        dbc = DBconnection("Neo4j", "")
        q = 'MATCH ({id:{source}})-[]->(r)-[]->({id:{target}})' \
            ' RETURN r.name'
        for source, target, n in tests:
            r = list(dbc.neo4jc.run(q, source=source, target=target))
            assert len(r) == n
            assert r[0]['r.name'] == '2-oxoglutarate + glycine <?>' \
                                     ' L-glutamate + glyoxylate'

    def test_neo4j_shortestpathsearch_connected_metabolites(self):
        nqry = QueryIntEnz("Neo4j")
        tests = [("2-oxoglutarate", "glyoxylate", 1),
                 ("malonate", "malonyl-CoA", 2)
                 ]
        for source, target, n in tests:
            r = nqry.neo4j_shortestpathsearch_connected_metabolites(source,
                                                                    target)
            r = list(r)
            assert n == len(r)
            for path in r:
                path = path['path']
                assert source == path.start.properties["id"]
                assert target == path.end.properties["id"]
                assert 2 == len(path.relationships)
                assert "Reactant_in" == path.relationships[0].type
                assert source == path.nodes[0].properties['id']
                assert "Produces" == path.relationships[-1].type
                assert target == path.nodes[-1].properties['id']

    def test_neo4j_getreactions(self):
        nqry = QueryIntEnz("Neo4j")
        r = list(nqry.getreactions())
        self.assertAlmostEqual(6250, len(r), delta=200)

    def test_mdb_getreactions(self):
        qc = {'$text': {'$search': '"poly(A)"'}}
        r = list(qryintenz.getreactions(qc))
        assert 1 == len(r)
        qc = {'$text': {'$search': '"oxopropanoate" "malonyl"'}}
        r = list(qryintenz.getreactions(qc))
        assert 2 == len(r)
        qc = {'$text': {'$search': 'oxopropanoate malonyl'}}
        r = list(qryintenz.getreactions(qc))
        self.assertAlmostEqual(90, len(r), delta=20)
        qc = {'$text': {'$search': 'semialdehyde'}}
        r = list(qryintenz.getreactions(qc))
        self.assertAlmostEqual(70, len(r), delta=20)


if __name__ == '__main__':
    unittest.main()

#!/usr/bin/env python
""" Test queries with IntEnz data indexed with MongoDB and Neo4j"""

import unittest
from nosqlbiosets.dbutils import DBconnection
from .query import QueryIntEnz

qryintenz = QueryIntEnz()


class TestQueryIntEnz(unittest.TestCase):

    def test_getreactant_product_names(self):
        re = qryintenz.getreactantnames()
        pr = qryintenz.getproductnames()
        i = set(re).intersection(pr)
        print("#Chemicals both in reactants and products = %d" % len(i))
        assert "cytidine" in i

    def test_enzymeswithreactant(self):
        tests = [
            # reactant, name of one expected enzyme, # of expected enzymes
            ("3-oxopropanoate", "Malonate-semialdehyde dehydrogenase", 5),
            ("glycerol", "D/L-glyceraldehyde reductase", 8),
            ("D-threo-isocitrate", "Isocitrate--homoisocitrate dehydrogenase",
             3)
        ]
        for r, e, n in tests:
            enzymes = qryintenz.getenzymeswithreactant(r)
            assert e in [e[1] for e in enzymes]
            assert n == len(enzymes)

    def test_enzymeswithreactant_chebiid(self):
        tests = [
            (32682, "3.5.3.6", "Arginine deiminase", 22)
        ]
        for chebiid, ecn, ename, n in tests:
            enzymes = qryintenz.getenzymeswithreactant_chebiid(chebiid)
            assert (ecn, ename) in enzymes
            assert n == len(enzymes)

    def test_enzymeswithproduct_chebiid(self):
        tests = [
            (32682, "3.6.3.21", "Polar-amino-acid-transporting ATPase", 5)
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
            ("2-oxoglutarate", "D-threo-isocitrate",
             "Isocitrate--homoisocitrate dehydrogenase")
        ]
        for source, target, enz in ste:
            r = qryintenz.query_reactantandproduct(source, target)
            assert len(r) >= 1
            assert enz in r

    def test_lookup_connected_metabolites(self):
        source, target, e1, e2 = "2-oxoglutarate", "glyoxylate",\
                                "LL-diaminopimelate aminotransferase",\
                                "Glycine oxidase"  # EC 2.6.1.83 -> 1.4.3.19
        r = qryintenz.lookup_connected_metabolites(source, target)
        assert e1 in [e["_id"] for e in r]
        assert e2 in {e["_id"]: e["enzyme2"] for e in r}[e1]

    def test_graphlookup_two_connected_metabolites(self):
        source, target, e1, e2 = "2-oxoglutarate", "glyoxylate",\
                                "LL-diaminopimelate aminotransferase",\
                                "Glycine transaminase"  # EC 2.6.1.83 -> 2.6.1.4
        r = qryintenz.graphlookup_connected_metabolites(source, target, 0)
        assert e1 in [e["_id"] for e in r]
        assert e2 in {e["_id"]: e["enzymes"] for e in r}[e1]

    def test_neo4j_graphsearch_with_two_connected_metabolites(self):
        source, target = "2-oxoglutarate", "glyoxylate"
        dbc = DBconnection("Neo4j", "")
        q = 'MATCH ({id:{source}})-[]->(r)-[]->({id:{target}})' \
            ' RETURN r.name'
        r = list(dbc.neo4jc.run(q, source=source, target=target))
        assert len(r) > 0
        assert r[0]['r.name'] == '2-oxoglutarate + glycine <?>' \
                                 ' L-glutamate + glyoxylate'

    def test_neo4j_shortestpathsearch_with_two_connected_metabolites(self):
        nqry = QueryIntEnz("Neo4j")
        source, target = "2-oxoglutarate", "glyoxylate"
        r = list(nqry.neo4j_shortestpathsearch_connected_metabolites(source,
                                                                     target))
        assert len(r) > 0
        path = r[0]['path']
        assert path.start.properties == {"id": "2-oxoglutarate"}
        assert path.end.properties == {"id": "glyoxylate"}
        assert path.relationships[0].type == "Reactant_in"
        assert len(path.nodes) == 3
        assert len(path.relationships) == 2

    def test_neo4j_getreactions(self):
        nqry = QueryIntEnz("Neo4j")
        r = list(nqry.getreactions())
        self.assertAlmostEqual(6250, len(r), delta=200)

    def test_mdb_getreactions(self):
        qc = {'$text': {'$search': 'semialdehyde'}}
        r = list(qryintenz.getreactions(qc))
        assert len(r) == 64


if __name__ == '__main__':
    unittest.main()

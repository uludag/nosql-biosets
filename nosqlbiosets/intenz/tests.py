#!/usr/bin/env python
""" Test queries with IntEnz data indexed with MongoDB """

import unittest

from .query import QueryIntEnz

qryintenz = QueryIntEnz()


class TestQueryIntEnz(unittest.TestCase):

    def test_getreactant_product_names(self):
        re = qryintenz.getreactantnames()
        pr = qryintenz.getproductnames()
        i = set(re).intersection(pr)
        print("#Chemicals both in reactants and products = %d" % len(i))
        assert "cytidine" in i

    def test_query_reactants(self):
        rnames = {
            "glycerol": ["D/L-glyceraldehyde reductase"],
            "D-threo-isocitrate": ["Isocitrate--homoisocitrate dehydrogenase"]
        }
        for name in rnames:
            enzymes = qryintenz.getenzymeswithreactants([name])
            assert rnames[name][0] in [
                e[1] for e in enzymes]

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


if __name__ == '__main__':
    unittest.main()

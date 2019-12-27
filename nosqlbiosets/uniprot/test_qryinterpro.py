#!/usr/bin/env python
""" Query InterPro data indexed with MongoDB """

from nosqlbiosets.uniprot.qryinterpro import QueryInterPro


class TestQueryInterPro:
    qri = QueryInterPro("MongoDB", "biosets", "interpro")

    def test_pfamid2names(self):
        r = self.qri.id2names(["PF00051", "PF00008", "PF04257"])
        assert r["PF00008"] == 'EGF-like domain'
        assert r["PF04257"] == 'RecBCD enzyme subunit RecC'
        assert r["PF00051"] == 'Kringle'

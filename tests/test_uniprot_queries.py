#!/usr/bin/env python
""" Test queries with UniProt data indexed with MongoDB or Elasticsearch """

import unittest

from nosqlbiosets.uniprot.query import QueryUniProt

qryuniprot = QueryUniProt("MongoDB", "biosets", "protein")
qryuniprot_es = QueryUniProt("Elasticsearch", "uniprot", "protein")


class TestQueryUniProt(unittest.TestCase):
    index = "biosets"
    doctype = "protein"

    def test_keggid_queries_es(self):
        db = "Elasticsearch"
        mids = qryuniprot_es.getnamesforkegggeneids(
            ['hsa:7157', 'hsa:121504'], db)
        self.assertSetEqual(set(mids), {'P53_HUMAN', 'H4_HUMAN'})

    def test_keggid_queries_mdb(self):
        db = "MongoDB"
        mids = qryuniprot.getnamesforkegggeneids(['hsa:7157', 'hsa:121504'], db)
        self.assertSetEqual(set(mids), {'P53_HUMAN', 'H4_HUMAN'})

    def test_genes_linkedto_keggreaction(self, db="MongoDB"):
        keggids = [('R01047', {'dhaB'}), ('R03119', {'dhaT'})]
        if db == "MongoDB":
            for keggid, genes in keggids:
                self.assertSetEqual(genes, qryuniprot.
                                    genes_linkedto_keggreaction(keggid))

    def test_getenzymedata(self):
        enzys = [('2.2.1.11', {'Q58980'}, {'MJ1585'},
                  "Aromatic compound metabolism.",
                  'D-fructose 1,6-bisphosphate = glycerone phosphate'
                  ' + D-glyceraldehyde 3-phosphate.',
                  'Methanococcus jannaschii'),
                 ('2.5.1.-', {'Q3J5F9'}, {'ctaB'},
                  "Alkaloid biosynthesis.",
                  '2,5-DCHQ = chlorohydroquinone + chloride.',
                  'Arabidopsis thaliana'),
                 ('5.4.2.2', {'P93804'}, {'PGM1'},
                  "Glycolipid metabolism;"
                  " diglucosyl-diacylglycerol biosynthesis.",
                  'Alpha-D-ribose 1-phosphate = D-ribose 5-phosphate.',
                  'Baker\'s yeast')]
        for ecn, accs, genes, pathway, reaction, org in enzys:
            assert genes.issubset(set(qryuniprot_es.getgenes(ecn)))
            assert genes.issubset(set(qryuniprot.getgenes(ecn)))
            assert accs.issubset(qryuniprot.getaccs(ecn))
            assert pathway in qryuniprot.getpathways(ecn)
            assert reaction in qryuniprot.getcatalyticactivity(ecn)
            assert org in qryuniprot_es.getorganisms(ecn)


if __name__ == '__main__':
    unittest.main()

#!/usr/bin/env python
""" Test queries with UniProt data indexed with MongoDB or Elasticsearch """

import unittest

from nosqlbiosets.uniprot.query import QueryUniProt

qryuniprot = QueryUniProt("MongoDB", "biosets", "uniprot")
qryuniprot_es = QueryUniProt("Elasticsearch", "uniprot", "uniprot")


class TestQueryUniProt(unittest.TestCase):

    def test_kegg_geneid_queries_es(self):
        db = "Elasticsearch"
        mids = qryuniprot_es.getnamesforkegg_geneids(
            ['hsa:7157', 'hsa:121504'], db)
        self.assertSetEqual(set(mids), {'P53_HUMAN', 'H4_HUMAN'})

    def test_kegg_geneid_queries_mdb(self):
        db = "MongoDB"
        ids = qryuniprot.getnamesforkegg_geneids(['hsa:7157', 'hsa:121504'], db)
        self.assertSetEqual(set(ids), {'P53_HUMAN', 'H4_HUMAN'})

    def test_genes_linkedto_keggreaction(self, db="MongoDB"):
        keggids = [('R01047', {'dhaB'}), ('R03119', {'dhaT'})]
        if db == "MongoDB":
            for keggid, genes in keggids:
                self.assertSetEqual(genes, qryuniprot.
                                    genes_linkedto_keggreaction(keggid))

    def test_get_lca(self):
        tests = [
            (['CLPC1_ARATH', 'CLPB_GLOVI', 'CLPC2_ORYSJ', 'CLPB_CHLCV'], None),
            (['RPOB_RHOS1', 'RPOB_RHOS4', 'RPOB_RHOSK', 'RPOB_RHOS5'],
             'Rhodobacter')]
        for ids, taxon in tests:
            r = qryuniprot.get_lca({'_id': {"$in": ids}})
            r = list(r)
            if taxon is None:
                assert [] == r
            else:
                assert taxon == r[-1]

    def test_getgenes(self):
        tests = [
            (['CLPC1_ARATH', 'CLPB_GLOVI', 'CLPC2_ORYSJ', 'CLPB_CHLCV'],
             {"clpB": 2, "CLPC2": 1, "CLPC1": 1}),
            (['RPOB_RHOS1', 'RPOB_RHOS4', 'RPOB_RHOSK', 'RPOB_RHOS5'],
             {"rpoB": 3, "rpoB1": 1, "rpoB2": 1})
        ]
        for ids, genes in tests:
            r = qryuniprot.getgenes(None, qc={'_id': {"$in": ids}})
            assert genes == r['primary']

    # Distribution of evidence codes in a text query result set
    def test_evidence_codes(self):
        ecodes = {  # http://www.uniprot.org/help/evidences
            255: 3350,  # match to sequence model evidence, manual assertion
            269: 4640,  # experimental evidence used in manual assertion
            305: 3710,  # curator inference used in manual assertion
            250: 2770,  # sequence similarity evidence used in manual assertion
            303: 1210,  # non-traceable author statement, manual assertion
            244: 650,   # combinatorial evidence used in manual assertion
            312: 630    # imported information used in manual assertion
        }
        qc = {'$text': {'$search': 'antimicrobial'}}
        self.assertAlmostEqual(4070,
                               len(list(qryuniprot.query(qc, {'_id': 1}))),
                               delta=4)
        aggqc = [
            {"$match": qc},
            {"$unwind": "$evidence"},
            {'$group': {
                '_id': '$evidence.type',
                "sum": {"$sum": 1}}}
        ]
        hits = qryuniprot.aggregate_query(aggqc)
        r = [c for c in hits]
        assert 7 == len(r)
        for i in r:
            self.assertAlmostEqual(ecodes[int(i['_id'][8:])], i['sum'], delta=8)

    def test_getenzymedata(self):
        enzys = [('2.2.1.11', {'Q58980'}, ("ordered locus", 'MJ1585', 1),
                  "Aromatic compound metabolism.",
                  'D-fructose 1,6-bisphosphate = glycerone phosphate'
                  ' + D-glyceraldehyde 3-phosphate.',
                  'Methanococcus jannaschii', 'common', 1),
                 ('2.5.1.-', {'Q3J5F9'}, ("primary", 'ctaB', 331),
                  "Cofactor biosynthesis; ubiquinone biosynthesis.",
                  '2,5-dichlorohydroquinone + 2 glutathione ='
                  ' chloride + chlorohydroquinone + glutathione disulfide.',
                  'Arabidopsis thaliana', 'scientific', 19),
                 ('5.4.2.2', {'P93804'}, ("primary", 'PGM1', 10),
                  "Glycolipid metabolism;"
                  " diglucosyl-diacylglycerol biosynthesis.",
                  'Alpha-D-ribose 1-phosphate = D-ribose 5-phosphate.',
                  'Baker\'s yeast', 'common', 2)
                 ]
        for ecn, accs, gene, pathway, reaction, org, nametype, n in enzys:
            r = qryuniprot_es.getgenes(ecn)
            assert gene[2] == r[gene[0]][gene[1]]
            r = qryuniprot.getgenes(ecn)
            assert gene[2] == r[gene[0]][gene[1]]
            assert accs.issubset(qryuniprot.getaccs(ecn))
            r = list(qryuniprot.getpathways(ecn))
            assert pathway in [pw['_id'] for pw in r]
            r = qryuniprot.getcatalyticactivity(ecn)
            assert reaction in [rc['_id'] for rc in r]
            assert n == qryuniprot_es.getorganisms(ecn, limit=20)[nametype][org]
            assert n == qryuniprot.getorganisms(ecn, limit=20)[nametype][org]


if __name__ == '__main__':
    unittest.main()

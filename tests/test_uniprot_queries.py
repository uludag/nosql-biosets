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

    # Distribution of evidence codes in a test query result set
    def test_evidence_codes(self):
        qc = {'$text': {'$search': 'antimicrobial'}}
        ecodes = {  # http://www.uniprot.org/help/evidences
            255: 3343,  # match to sequence model evidence, manual assertion
            269: 4627,  # experimental evidence used in manual assertion
            305: 3699,  # curator inference used in manual assertion
            250: 2770,  # sequence similarity evidence used in manual assertion
            303: 1209,  # non-traceable author statement, manual assertion
            244: 651,   # combinatorial evidence used in manual assertion
            312: 626    # imported information used in manual assertion
        }
        assert 4067 == len(list(qryuniprot.query(qc, {'_id': 1})))
        aggqc = [
            {"$match": qc},
            {"$unwind": "$evidence"},
            {'$group': {
                '_id': '$evidence.type',
                "sum": {"$sum": 1}}}
        ]
        hits = qryuniprot.aggregate_query(aggqc)
        r = [c for c in hits]
        print(r)
        assert 7 == len(r)
        for i in r:
            assert ecodes[int(i['_id'][8:])] == i['sum']

    def test_getenzymedata(self):
        enzys = [('2.2.1.11', {'Q58980'}, {'MJ1585'},
                  "Aromatic compound metabolism.",
                  'D-fructose 1,6-bisphosphate = glycerone phosphate'
                  ' + D-glyceraldehyde 3-phosphate.',
                  'Methanococcus jannaschii'),
                 ('2.5.1.-', {'Q3J5F9'}, {'ctaB'},
                  "Alkaloid biosynthesis.",
                  '2,5-dichlorohydroquinone + 2 glutathione ='
                  ' chloride + chlorohydroquinone + glutathione disulfide.',
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

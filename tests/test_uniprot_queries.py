#!/usr/bin/env python
""" Test queries with UniProt data indexed with MongoDB and Elasticsearch """

import unittest

from nosqlbiosets.uniprot.query import QueryUniProt

qryuniprot = QueryUniProt("MongoDB", "biosets", "uniprot")


class TestQueryUniProt(unittest.TestCase):

    def test_kegg_geneid_queries_es(self):
        ids = qryuniprot.getnamesforkegg_geneids(
            ['hsa:7157', 'hsa:121504'], "Elasticsearch")
        self.assertListEqual(['H4_HUMAN', 'P53_HUMAN'], sorted(set(ids)))

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
        qc = {
            "dbReference": {"$elemMatch": {
                "type": "EC",
                "id": "4.2.1.-"
            }}}
        assert len(qryuniprot.get_lca(qc)) == 0
        tests = [
            (['CLPC1_ARATH', 'CLPB_GLOVI', 'CLPC2_ORYSJ', 'CLPB_CHLCV'], None),
            (['RPOB_RHOS1', 'RPOB_RHOS4', 'RPOB_RHOSK', 'RPOB_RHOS5'],
             'Rhodobacter')]
        for ids, taxon in tests:
            r = qryuniprot.get_lca({'_id': {"$in": ids}})
            if taxon is None:
                assert [] == r
            else:
                assert taxon == r[-1]

    def test_get_species(self):
        ecn = "4.2.1.-"
        qc = {
            "dbReference": {"$elemMatch": {
                "type": "EC",
                "id": {'$regex': '^' + ecn[:-1]}
            }}}
        r = qryuniprot.getspecies(qc)
        n = len(r)
        qc['dbReference']["$elemMatch"]["id"] = ecn
        r = qryuniprot.getspecies(qc)
        m = len(r)
        assert m < n

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
            255: 4025,  # match to sequence model evidence, manual assertion
            269: 6040,  # experimental evidence used in manual assertion
            305: 4380,  # curator inference used in manual assertion
            250: 3088,  # sequence similarity evidence used in manual assertion
            303: 1650,  # non-traceable author statement, manual assertion
            244: 790,   # combinatorial evidence used in manual assertion
            312: 748    # imported information used in manual assertion
        }
        qc = {'$text': {'$search': 'antimicrobial'}}
        self.assertAlmostEqual(4700,
                               len(list(qryuniprot.query(qc, {'_id': 1}))),
                               delta=100)
        aggqc = [
            {"$match": qc},
            {"$unwind": "$evidence"},
            {'$group': {
                '_id': '$evidence.type',
                "sum": {"$sum": 1}}}
        ]
        cr = qryuniprot.aggregate_query(aggqc)
        for i in cr:
            self.assertAlmostEqual(ecodes[int(i['_id'][8:])], i['sum'],
                                   delta=100)
        # Distribution of evidence types in the same example query
        etypes = {
            "evidence at protein level": 2459,
            "evidence at transcript level": 633,
            "inferred from homology": 1587,
            "predicted": 6,
            "uncertain": 29
        }
        aggqc = [
            {"$match": qc},
            {'$group': {
                '_id': '$proteinExistence.type',
                "sum": {"$sum": 1}}}
        ]
        hits = qryuniprot.aggregate_query(aggqc)
        r = {c['_id']: c['sum'] for c in hits}
        assert r == etypes

    # Distribution of GO annotations
    def test_GO_annotations(self):
        tests = [  # species, unique annotations, all annotations
            ('Rice', 2786, 25482),
            ('Human', 18055, 263951),
            ('Arabidopsis thaliana', 6741, 99198),
            ('Danio rerio', 5160, 22491)
        ]
        for org, uniqgo, nall in tests:
            qc = {'organism.name.#text': org}
            aggqc = [
                {"$match": qc},
                {"$unwind": "$dbReference"},
                {"$match": {"dbReference.type": "GO"}},
                {'$group': {
                    '_id': {
                        'id': '$dbReference.id',
                        'name': {"$arrayElemAt": ['$dbReference.property', 0]}
                    },
                    "abundance": {"$sum": 1}
                }},
                {"$sort": {"abundance": -1}},
                {'$project': {
                    "abundance": 1,
                    "id": "$_id.id",
                    "name": "$_id.name.value",
                    "_id": 0
                }}
            ]
            hits = qryuniprot.aggregate_query(aggqc)
            r = [c for c in hits]
            self.assertAlmostEqual(uniqgo, len(r), delta=100)
            self.assertAlmostEqual(nall, sum([c['abundance'] for c in r]),
                                   delta=1000)

    def test_getenzymedata(self):
        enzys = [
            ('2.2.1.11', {'Q58980'}, ("ordered locus", 'MJ1585', 1),
             "Aromatic compound metabolism.",
             'beta-D-fructose 1,6-bisphosphate = D-glyceraldehyde 3-phosphate'
             ' + dihydroxyacetone phosphate',
             'Methanococcus jannaschii', 'common', 1, 1),
            ('2.5.1.-', {'Q5AR51'}, ("primary", 'ubiA', 224),
             "Cofactor biosynthesis; ubiquinone biosynthesis.",
             'hydrogen sulfide + O-acetyl-L-serine = acetate + L-cysteine',
             'Arabidopsis thaliana', 'scientific', 18, 500),
            ('5.4.2.2', {'P93804'}, ("primary", 'PGM1', 10),
             "Glycolipid metabolism;"
             " diglucosyl-diacylglycerol biosynthesis.",
             'alpha-D-ribose 1-phosphate = D-ribose 5-phosphate',
             'Baker\'s yeast', 'common', 2, 25)
        ]
        qryuniprot_es = QueryUniProt("Elasticsearch", "uniprot", "uniprot")
        for ecn, accs, gene, pathway, reaction, org, nametype, n, orgs in enzys:
            genetype, genename, abundance = gene
            r = qryuniprot_es.getgenes(ecn, limit=100)  # Elasticsearch
            assert abundance == r[genetype][genename], r
            r = qryuniprot.getgenes(ecn)
            assert abundance == r[genetype][genename]
            assert accs.issubset(qryuniprot.getaccs(ecn))
            r = list(qryuniprot.getpathways(ecn))
            assert pathway in [pw['_id'] for pw in r]
            r = qryuniprot.getcatalyticactivity(ecn)
            assert reaction in [rc['_id'] for rc in r], ecn
            for organisms in [qryuniprot_es.getorganisms(ecn, limit=2000),
                              qryuniprot.getorganisms(ecn, limit=2000)]:
                assert n == organisms[nametype][org], organisms
                self.assertAlmostEqual(orgs, len(organisms[nametype]), delta=30)


if __name__ == '__main__':
    unittest.main()

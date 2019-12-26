#!/usr/bin/env python
""" Test queries with UniProt data indexed with MongoDB and Elasticsearch """

import unittest

from nosqlbiosets.uniprot.query import QueryUniProt, idmatch

MDB_COLLECTION = "uniprot"
ESINDEX = "uniprot"
MDBHOST = "tests.cbrc.kaust.edu.sa"
ESHOST = "tests.cbrc.kaust.edu.sa"

qryuniprot = QueryUniProt("MongoDB", "biosets", MDB_COLLECTION,
                          host=MDBHOST)
qryuniprot_es = QueryUniProt("Elasticsearch", ESINDEX, "",
                             host=ESHOST, port=9200)


class TestQueryUniProt(unittest.TestCase):

    def test_kegg_geneid_queries_es(self):
        ids = qryuniprot_es.getnamesforkegg_geneids(
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
                r = qryuniprot.genes_linkedto_keggreaction(keggid)
                self.assertSetEqual(genes, r, msg=r)

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
             {"clpB": 2, "CLPC2": 1, "CLPC1": 1},
             {2601981, 4351828, 835165}),
            (['RPOB_RHOS1', 'RPOB_RHOS4', 'RPOB_RHOSK', 'RPOB_RHOS5'],
             {"rpoB": 3, "rpoB1": 1, "rpoB2": 1}, {3718913})
        ]
        for ids, genes, egids in tests:
            r = qryuniprot.getgenes(None, qc={'_id': {"$in": ids}})
            assert genes == r['primary']

    def test_getgeneids(self):
        tests = [
            ({"accession": "Q16613"}, 1, 15),
            # ({"organism.dbReference.id": "9606"}, 19022, 15)
        ]
        for qc, nids, entrezid in tests:
            r = qryuniprot.getgeneids(qc, limit=20000)
            assert len(r) == nids
            assert entrezid == r.pop()[1]
        tests = [
            ({"name": "BIEA_HUMAN"}, 1, ("BIEA_HUMAN", 644, 'BLVRA')),
            ({'$text': {'$search': 'bilirubin'},
              "organism.dbReference.id": "9606"},
             18, ("BIEA_HUMAN", 644, 'BLVRA'))
        ]
        for qc, nids, ids in tests:
            r = qryuniprot.getgeneids(qc, limit=20000)
            assert len(r) == nids
            assert ids in r

    def test_idmatch(self):
        tests = [
            ("645, CASQ2, GSTO1, DMD, GSTM2, MALAT1, 2168, UD14_HUMAN, BVR",
             8, ("BLVRB_HUMAN", 645, 'BLVRB')),
            ("BIEA_HUMAN", 1, ("BIEA_HUMAN", 644, 'BLVRA')),
            ("BLVRB_HUMAN", 1, ("BLVRB_HUMAN", 645, 'BLVRB')),
            ("BLVRB", 1, ("BLVRB_HUMAN", 645, 'BLVRB'))
        ]
        for iids, nids, ids in tests:
            r = idmatch(iids, limit=20000, mdbcollection=MDB_COLLECTION,
                        host=MDBHOST)
            assert len(r) == nids
            assert ids in r

    # Distribution of evidence codes in a text query result set
    def test_evidence_codes(self):
        ecodes = {  # http://www.uniprot.org/help/evidences
            255: 4025,  # match to sequence model evidence, manual assertion
            269: 6170,  # experimental evidence used in manual assertion
            305: 4503,  # curator inference used in manual assertion
            250: 3245,  # sequence similarity evidence used in manual assertion
            303: 1805,  # non-traceable author statement, manual assertion
            244: 891,   # combinatorial evidence used in manual assertion
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
        # Distribution of evidence types for the same example query
        etypes = {
            "evidence at protein level": 2495,
            "evidence at transcript level": 629,
            "inferred from homology": 1606,
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

    # Distribution of Pfam annotations
    def test_Pfam_annotations(self):
        e = [{'abundance': 200, 'id': 'PF00009', 'name': 'GTP_EFTU'},
             {'abundance': 200, 'id': 'PF00005', 'name': 'ABC_tran'},
             {'abundance': 180, 'id': 'PF04055', 'name': 'Radical_SAM'},
             {'abundance': 170, 'id': 'PF00069', 'name': 'Pkinase'}]
        qc = {"$sample": {'size': 26000}}
        r = qryuniprot.getannotations(qc, annottype='Pfam')
        r = list(r)
        self.assertAlmostEqual(len(r), 4700, delta=600)  # distinct Pfam ids
        self.assertAlmostEqual(sum([c['abundance'] for c in r]), 37000,
                               delta=2000)
        r = {i['id']: i['abundance'] for i in r}
        for i in e:
            self.assertAlmostEqual(r[i['id']], i['abundance'],
                                   delta=i['abundance']/2, msg=i)

    # Distribution of GO annotations
    def test_GO_annotations(self):
        qc = {"$sample": {'size': 20000}}
        r = qryuniprot.getannotations(qc, annottype="GO")
        r = list(r)
        self.assertAlmostEqual(len(r), 12500, delta=2000)  # distinct GO ids
        self.assertAlmostEqual(sum([c['abundance'] for c in r]), 122000,
                               delta=20000)

    def test_annotation_pairs(self):
        qc = {'organism.name.#text': 'Rice'}
        r = list(qryuniprot.top_annotation_pairs(qc))
        go = {i['_id']['go']['property'][0]['value'] for i in r[:3]}
        assert go == {
            'F:protein serine/threonine phosphatase activity',
            "F:magnesium-dependent protein serine/threonine phosphatase activity",
            'F:metal ion binding'}
        assert all(i['_id']['pfam']['property'][0]['value'] == 'PP2C'
                   for i in r[:3])

    def test_getenzymedata(self):
        enzys = [
            ('2.2.1.11', {'Q58980'}, ("ordered locus", 'MJ1585', 1),
             "Aromatic compound metabolism.",
             'beta-D-fructose 1,6-bisphosphate = D-glyceraldehyde 3-phosphate'
             ' + dihydroxyacetone phosphate',
             'Methanococcus jannaschii', 'common', 1, 1),
            ('2.5.1.-', {'Q5AR51'}, ("primary", 'UBIAD1', 3),
             "Cofactor biosynthesis; ubiquinone biosynthesis.",
             'hydrogen sulfide + O-acetyl-L-serine = acetate + L-cysteine',
             'Arabidopsis thaliana', 'scientific', 18, 430),
            ('5.4.2.2', {'P93804'}, ("primary", 'PGM1', 10),
             "Glycolipid metabolism;"
             " diglucosyl-diacylglycerol biosynthesis.",
             'alpha-D-ribose 1-phosphate = D-ribose 5-phosphate',
             'Baker\'s yeast', 'common', 2, 25)
        ]
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

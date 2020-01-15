#!/usr/bin/env python
""" Test queries with ClinVar data indexed with MongoDB """

import unittest

from nosqlbiosets.variation.qryclinvar import QueryClinVar

qry = QueryClinVar("MongoDB", "biosets", "clinvarvariation")


class TestQueryClinVar(unittest.TestCase):

    def testqry_topclinicalassertion_tests(self):
        aggq = [
            {'$match': {
                "InterpretedRecord.clinicalAssertion."
                "observedIn.Method.MethodAttribute.Attribute.Type": "TestName"}},
            {"$unwind": "$InterpretedRecord.clinicalAssertion"},
            {"$unwind": "$InterpretedRecord.clinicalAssertion.observedIn"},
            {'$match': {
                "InterpretedRecord.clinicalAssertion."
                "observedIn.Method.MethodAttribute.Attribute.Type": "TestName"}},
            {"$group": {
                "_id": {
                    "test": "$InterpretedRecord.clinicalAssertion."
                            "observedIn.Method.MethodAttribute.Attribute.#text",
                },
                "abundance": {"$sum": 1}
            }},
            {"$sort": {"abundance": -1}},
            {"$limit": 6},
            {"$project": {"test": "$_id.test",
                          "abundance": 1, "_id": 0}}
        ]
        cr = qry.aggregate_query(aggq)
        e = [
            ('BROCA', 690), ('Whole Exome Sequencing', 495),
            ('Genetic Testing for FH', 346), ('Exome Sequencing', 337),
            ('Gene Panel Sequencing', 194), ('WES', 154)
        ]
        r = {i['test']: i['abundance'] for i in cr}
        for test, a in e:
            self.assertAlmostEqual(r[test], a, delta=a/5, msg=test)

    def testqry_topclinicalassertion_methods(self):
        aggq = [
            {
                "$project": {"InterpretedRecord.clinicalAssertion."
                             "observedIn.Method.MethodType": 1}},
            {"$unwind": "$InterpretedRecord.clinicalAssertion"},
            {"$unwind": "$InterpretedRecord.clinicalAssertion.observedIn"},
            {"$group": {
                "_id": {
                    "method": "$InterpretedRecord.clinicalAssertion."
                              "observedIn.Method.MethodType",
                },
                "abundance": {"$sum": 1}
            }},
            {"$sort": {"abundance": -1}},
            {"$limit": 6},
            {"$project": {"method": "$_id.method",
                          "abundance": 1, "_id": 0}}
        ]
        cr = qry.aggregate_query(aggq)
        e = {
            'clinical testing': 794827, 'literature only': 50295,
            'research': 25520, 'curation': 24260,
            'reference population': 19785, 'not provided': 12023}
        r = {i['method']: i['abundance'] for i in cr}
        for method, ab in e.items():
            self.assertAlmostEqual(r[method], ab, delta=ab/5, msg=method)

    def testqry_variantsbysubmitter(self):
        aggq = [
            {"$group": {
                "_id": {
                    "submitter": "$InterpretedRecord.clinicalAssertion."
                                 "ClinVarAccession.SubmitterName",
                },
                "abundance": {"$sum": 1}
            }},
            {"$sort": {"abundance": -1}},
            {"$limit": 5},
            {"$project": {"submitter": "$_id.submitter",
                          "abundance": 1, "_id": 0}}
        ]
        cr = qry.aggregate_query(aggq)
        e = {
            'Invitae': 204758,
            'Illumina Clinical Services Laboratory,Illumina': 54115,
            'GeneDx': 66075,
            'EGL Genetic Diagnostics,Eurofins Clinical Diagnostics': 18857,
            "Laboratory of Genetics and Genomics,"
            "Cincinnati Children's Hospital Medical Center": 17786
        }
        r = {i['submitter'][0]: i['abundance'] for i in cr}
        for submitter, a in e.items():
            self.assertAlmostEqual(r[submitter], a, delta=a/10, msg=submitter)

    def testqry_varianteffectontologies(self):
        e = {
            "Variation Ontology": 165,
            "Sequence Ontology": 167}
        aggq = [
            {"$project": {'InterpretedRecord.clinicalAssertion.SimpleAllele'
                          '.FunctionalConsequence.XRef.DB':1}},
            {"$unwind": "$InterpretedRecord.clinicalAssertion"},
            {"$unwind": '$InterpretedRecord.clinicalAssertion.SimpleAllele'
                        '.FunctionalConsequence.XRef.DB'},
            {"$group": {
                "_id": {
                    "db": '$InterpretedRecord.clinicalAssertion.SimpleAllele'
                          '.FunctionalConsequence.XRef.DB'
                },
                "abundance": {"$sum": 1}
            }},
            {"$sort": {"abundance": -1}},
            {"$limit": 4}
        ]
        r = list(qry.aggregate_query(aggq))
        r = {i['_id']['db']: i['abundance'] for i in r}
        for ont in e:
            self.assertAlmostEqual(r[ont], e[ont], delta=10, msg=ont)

    def testqry_topvarianttypes(self):
        aggq = [
            {"$unwind": "$InterpretedRecord.SimpleAllele.HGVSlist.HGVS"},
            {"$group": {
                "_id": {
                    "db": '$InterpretedRecord.SimpleAllele'
                          '.HGVSlist.HGVS.MolecularConsequence.DB',
                    "type": '$InterpretedRecord.SimpleAllele'
                            '.HGVSlist.HGVS.MolecularConsequence.Type'
                },
                "abundance": {"$sum": 1}
            }},
            {"$sort": {"abundance": -1}},
            {"$limit": 8}
        ]
        cr = qry.aggregate_query(aggq)
        e = {
            ('--', '--'): 2467457,
            ('SO', 'missense variant'): 792383,
            ('SO', 'intron variant'): 418895,
            ('SO', 'synonymous variant'): 493542,
            ('SO', '3 prime UTR variant'): 134351,
            ('SO', 'frameshift variant'): 131261,
            ('SO', 'nonsense'): 81500,
            ('SO', '5 prime UTR variant'): 70966
        }
        r = {(i['_id']['db'] if 'db' in i['_id'] else '--',
              i['_id']['type'] if 'type' in i['_id'] else '--'):
                 i['abundance']
             for i in cr}
        self.comparepairs(r, e, r=14)

    def testqry_exampledistinctqueries(self):
        r = qry.distinct("InterpretedRecord.Interpretations."
                         "Interpretation.ConditionList.TraitSet.Type", {})
        assert set(r) == {"Disease", "Finding", "DrugResponse",
                          "PhenotypeInstruction", "TraitChoice"}

        r = qry.distinct("InterpretedRecord.Interpretations."
                         "Interpretation.ConditionList.TraitSet.Trait.Type", {})
        self.assertSetEqual(set(r), {'Disease', 'Finding', 'NamedProteinVariant',
                                     'DrugResponse', 'BloodGroup',
                                     'PhenotypeInstruction'})

        r = qry.distinct("InterpretedRecord.Interpretations."
                         "Interpretation.Type", {})
        assert r == [None, "Clinical significance"]
        # None: when top record is IncludedRecord

        r = qry.distinct('InterpretedRecord.SimpleAllele'
                         '.HGVSlist.HGVS.MolecularConsequence.Type', {})
        self.assertSetEqual(set(r), {
            "missense variant",
            "non-coding transcript variant", "nonsense", "intron variant",
            "splice acceptor variant", "frameshift variant",
            "splice donor variant", "synonymous variant", "5 prime UTR variant",
            "3 prime UTR variant", "stop lost",
            'downstream transcript variant',
            'genic downstream transcript variant',
            'inframe_deletion',
            'initiatior codon variant',
            'upstream transcript variant',
            'no sequence alteration',
            'genic upstream transcript variant',
            'inframe_insertion',
            'inframe_indel'
        })
        r = qry.distinct('InterpretedRecord'
                         '.SimpleAllele.FunctionalConsequence.XRef.DB', {})
        self.assertSetEqual(set(r), {"Variation Ontology", "Sequence Ontology"})

        r = qry.distinct('InterpretedRecord.clinicalAssertion'
                         '.SimpleAllele.FunctionalConsequence.XRef.DB', {})
        self.assertSetEqual(set(r), {"Variation Ontology", "Sequence Ontology"})

        r = qry.distinct('RecordStatus', {})
        assert r == ["current"]

        r = qry.distinct('Species', {})
        assert r == ['Homo sapiens']

    def testqry_topinterpretationspersubmitter(self):
        e = {('not provided', 'DeBelle Laboratory for Biochemical Genetics,'
                              ' MUHC/MCH RESEARCH INSTITUTE'): 241,
             ('Likely pathogenic', 'Inserm U 954,'
                                   ' Faculté de Médecine de Nancy'): 39,
             ('Likely pathogenic', 'DeBelle Laboratory for Biochemical Genetics,'
                                   ' MUHC/MCH RESEARCH INSTITUTE'): 69,
             ('Likely pathogenic', 'Counsyl'): 74,
             ('Uncertain significance', 'Counsyl'): 46}
        r = qry.topinterpretationspersubmitter(
            {'InterpretedRecord.SimpleAllele.GeneList.Gene.Symbol': 'PAH'})
        self.comparepairs(r, e)

    def testqry_topinterpretationspergene(self):
        """Genes with most variant interpretations"""
        e = {('-', 'Benign'): 7644,
             ('BRCA2', 'Uncertain significance'): 4187,
             (('TTN', 'TTN-AS1'), 'Uncertain significance'): 3399,
             ('-', 'Uncertain significance'): 3371,  # '-' means no gene defined
             ('BRCA2', 'Pathogenic'): 3300,
             ('APC', 'Uncertain significance'): 2925,
             ('BRCA1', 'Pathogenic'): 2861,
             ('TTN', 'Uncertain significance'): 2847,
             ('BRCA1', 'Uncertain significance'): 2286,
             ('ATM', 'Uncertain significance'): 2153}
        r = qry.topinterpretationspergene({}, limit=10)
        r_ = dict()
        for i in r:
            _id, ab = i['_id'], i['abundance']
            if 'gene' in _id:
                if isinstance(_id['gene'], list):
                    gene = tuple(_id['gene'])
                else:
                    gene = _id['gene']
            else:
                gene = '-'
            r_[(gene, _id['desc'])] = ab
        self.comparepairs(r_, e)

    def testqry_geneswithmostvariants(self):
        aggq = [
            {"$group": {
                "_id": {
                    "desc": '$InterpretedRecord.SimpleAllele.GeneList.Gene.'
                            'Symbol',
                },
                "abundance": {"$sum": 1}
            }},
            {"$sort": {"abundance": -1}},
            {"$limit": 6}
        ]
        cr = qry.aggregate_query(aggq)
        genes = [None, "BRCA2", "BRCA1", ['TTN', 'TTN-AS1'], "APC", "TTN"]
        assert [i['_id']['desc'] for i in cr] == genes

    def testqry_geneconditionpairswithmostvariants(self):
        aggq = [
            {'$match': {
                "InterpretedRecord.Interpretations.Interpretation"
                ".ConditionList.TraitSet.Trait.Name": {
                    '$type': 'array'}}},
            {"$unwind": '$InterpretedRecord.SimpleAllele.GeneList.Gene.Symbol'},
            {"$group": {
                "_id": {
                    "desc": '$InterpretedRecord.SimpleAllele.GeneList.Gene'
                            '.Symbol',
                    "trait": {
                        "$filter": {
                            "input": "$InterpretedRecord.Interpretations"
                                     ".Interpretation"
                                     ".ConditionList.TraitSet.Trait.Name",
                            "as": "r",
                            "cond": {
                                "$eq": ["$$r.ElementValue.Type", "Preferred"]}
                        }}
                },
                "abundance": {"$sum": 1}
            }},
            {"$sort": {"abundance": -1}},
            {"$limit": 10}
        ]
        cr = qry.aggregate_query(aggq)
        epairs = {
            ("BRCA2", "Breast-ovarian cancer, familial 2"): 2317,
            ("BRCA1", "Breast-ovarian cancer, familial 1"): 2207,
            ("NF1", "Neurofibromatosis, type 1"): 1980,
            ("LDLR", "Familial hypercholesterolemia"): 1969
        }
        pairs = {}
        for i in cr:
            if len(i['_id']['trait']) > 0:
                pairs[(i['_id']['desc'],
                       i['_id']['trait'][0]['ElementValue']['#text'])] = \
                    i['abundance']
        self.comparepairs(pairs, epairs)

    def comparepairs(self, pairs, epairs, r=5):
        for pair, ab in epairs.items():
            self.assertAlmostEqual(pairs[pair], ab,
                                   delta=ab / r, msg=pair)


if __name__ == '__main__':
    unittest.main()

#!/usr/bin/env python
""" Test queries with ClinVar data indexed with MongoDB """

import unittest

from nosqlbiosets.variation.qryclinvar import QueryClinVar

qry = QueryClinVar("MongoDB", "biosets", "clinvarvariation")


class TestQueryClinVar(unittest.TestCase):

    def testqry_topclinicalassertion_tests(self):
        aggq = [
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
        b = [
            ('BROCA', 690), ('Whole Exome Sequencing', 495),
            ('Genetic Testing for FH', 346), ('Exome Sequencing', 337),
            ('Gene Panel Sequencing', 194), ('WES', 154)
        ]
        r = [(i['test'], i['abundance']) for i in cr]
        assert r == b

    def testqry_topclinicalassertion_methods(self):
        aggq = [
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
        b = {
            'clinical testing': 788860, 'literature only': 50159,
            'research': 25012, 'curation': 24073,
            'reference population': 19785, 'not provided': 12023}
        r = {i['method']: i['abundance'] for i in cr}
        assert r == b

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
            'Invitae': 100003,
            'Illumina Clinical Services Laboratory,Illumina': 54162,
            'GeneDx': 66864,
            'EGL Genetic Diagnostics,Eurofins Clinical Diagnostics': 21521,
            "Laboratory of Genetics and Genomics,"
            "Cincinnati Children's Hospital Medical Center": 17786
        }
        r = {i['submitter'][0]: i['abundance'] for i in cr}
        assert r == e

    def testqry_varianteffectontologies(self):
        aggq = [
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
        cr = qry.aggregate_query(aggq)
        e = [([], 560409), (None, 607), (["Variation Ontology"], 160),
             (["Sequence Ontology"], 157)]
        r = [(i['_id']['db'], i['abundance']) for i in cr]
        assert r == e

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
        e = [
            ('--', '--', 2263255),
            ('SO', 'missense variant', 725110),
            ('SO', 'intron variant', 338265),
            ('SO', 'synonymous variant', 274825),
            ('SO', '3 prime UTR variant', 134071),
            ('SO', 'frameshift variant', 115472),
            ('SO', 'nonsense', 80942),
            ('SO', '5 prime UTR variant', 63925)
        ]
        r = [(i['_id']['db'] if 'db' in i['_id'] else '--',
              i['_id']['type'] if 'type' in i['_id'] else '--',
              i['abundance'])
             for i in cr]
        assert r == e

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
        e = [('not provided', ['DeBelle Laboratory for Biochemical Genetics,'
                               ' MUHC/MCH RESEARCH INSTITUTE'], 242),
             ('Likely pathogenic', ['Inserm U 954,'
                                    ' Faculté de Médecine de Nancy'], 32),
             ('Likely pathogenic', ['DeBelle Laboratory for Biochemical Genetics,'
                                    ' MUHC/MCH RESEARCH INSTITUTE', 'Counsyl'], 23),
             ('Uncertain significance', ['Counsyl'], 22),
             ('Likely pathogenic', ['Counsyl'], 22)]
        r = qry.topinterpretationspersubmitter(
            {'InterpretedRecord.SimpleAllele.GeneList.Gene.Symbol': 'PAH'}, 5)
        r = [(i['_id']['interpretation'],
              i['_id']['submitter'],
              i['abundance']) for i in r]
        assert r == e

    def testqry_topinterpretationspergene(self):
        """Genes with most variant interpretations"""
        e = [('-', 'Benign', 7645), ('BRCA2', 'Uncertain significance', 4186),
             (['TTN', 'TTN-AS1'], 'Uncertain significance', 3393),
             ('BRCA2', 'Pathogenic', 3301),
             ('-', 'Uncertain significance', 3251),  # '-' means no gene defined
             ('APC', 'Uncertain significance', 2927),
             ('BRCA1', 'Pathogenic', 2863),
             ('TTN', 'Uncertain significance', 2850),
             ('BRCA1', 'Uncertain significance', 2288),
             ('ATM', 'Uncertain significance', 2154)]
        r = qry.topinterpretationspergene({}, limit=10)
        r = [(i['_id']['gene'] if 'gene' in i['_id'] else '-',
              i['_id']['desc'] if 'desc' in i['_id'] else '-',
              i['abundance']) for i in r]
        assert r == e

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
            {"$limit": 5}
        ]
        cr = qry.aggregate_query(aggq)
        epairs = [
            ("BRCA2", "Breast-ovarian cancer, familial 2", 2321),
            ("BRCA1", "Breast-ovarian cancer, familial 1", 2213),
            ("LDLR", "Familial hypercholesterolemia", 2050),
            ("NF1", "Neurofibromatosis, type 1", 1982)
        ]
        pairs = []
        for i in cr:
            if len(i['_id']['trait']) > 0:
                pairs.append((i['_id']['desc'],
                              i['_id']['trait'][0]['ElementValue']['#text'],
                              i['abundance']))
        assert pairs == epairs


if __name__ == '__main__':
    unittest.main()

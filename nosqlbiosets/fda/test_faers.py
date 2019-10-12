#!/usr/bin/env python
""" Test queries with FDA Adverese Event Reports data on MongoDB indexes """
import unittest

from nosqlbiosets.fda.query import QueryFaers


class QueryFaersTests(unittest.TestCase):
    qry = QueryFaers(collection='faers')

    def test_get_adversereactions(self):
        # Numbers based on partially indexed 2019 reports
        e = {'Drug ineffective': 7409, 'Death': 4823, 'Off label use': 3626,
             'Nausea': 3158, 'Fatigue': 3129, 'Headache': 2978,
             'Product dose omission': 2942, 'Malaise': 2631,
             'Dyspnoea': 2551, 'Diarrhoea': 2506}
        qc = {}
        r = self.qry.get_adversereactions(qc)
        r = {i['_id']['reaction']: i['abundance'] for i in r}
        self.maxDiff = None
        self.assertDictEqual(r, e)

    def test_reaction_medicine_pairs(self):
        # Numbers based on partially indexed 2019 reports
        e = {('Chronic kidney disease', 'NEXIUM'): 1229,
             ('Renal injury', 'NEXIUM'): 804,
             ('Chronic kidney disease', 'PRILOSEC'): 731,
             ('Hereditary angioedema', 'HAEGARDA'): 718,
             ('Premature delivery', 'MAKENA'): 706,
             ('Drug ineffective', 'COSENTYX'): 706,
             ('Premature baby', 'MAKENA'): 681,
             ('Renal injury', 'PRILOSEC'): 628,
             ('Psoriasis', 'COSENTYX'): 578,
             ('Economic problem', 'ABILIFY'): 548}
        qc = {}
        r = self.qry.get_reaction_medicine_pairs(qc)
        r = {(i['_id']['reaction'], i['_id']['medicine']): i['abundance']
             for i in r}
        self.maxDiff = None
        self.assertDictEqual(r, e)

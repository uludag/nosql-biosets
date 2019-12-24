#!/usr/bin/env python
""" Test queries with FDA Adverese Event Reports data on MongoDB indexes """
import datetime
import unittest

from nosqlbiosets.fda.query import QueryFaers


class QueryFaersTests(unittest.TestCase):
    qry = QueryFaers(dbtype="MongoDB", index="biosets", mdbcollection='faers')

    def test_get_adversereactions(self):
        # Numbers based on 2019 quarter 1 reports
        e = {'Drug ineffective': 28233, 'Death': 14136, 'Off label use': 13385,
             'Nausea': 13665, 'Fatigue': 13960, 'Headache': 11626,
             'Drug hypersensitivity': 10161,
             'Product dose omission': 9899, 'Malaise': 9688, 'Pain':9300,
             'Dyspnoea': 10339, 'Diarrhoea': 11977}
        d1 = datetime.datetime(2019, 1, 1, 0)
        d2 = datetime.datetime(2019, 3, 31, 23)
        qc = {"receiptdate": {"$gt": d1, "$lt": d2}}
        r = self.qry.get_adversereactions(qc, limit=12)
        r = {i['_id']['reaction']: i['abundance'] for i in r}
        self.maxDiff = None
        self.assertDictEqual(r, e)

    def test_reaction_medicine_pairs(self):
        # Numbers based on 2019 quarter 1 reports, first 20 days of 3rd month
        e = {('Chronic kidney disease', 'NEXIUM'): 1230,
             ('Drug ineffective', 'METHOTREXATE.'): 1015,
             ('Tremor', 'RYTARY'): 971, ('Asthma', 'XOLAIR'): 873,
             ('Renal injury', 'NEXIUM'): 816,
             ('Drug ineffective', 'ENBREL'): 748, ('Malaise', 'XOLAIR'): 744,
             ('Chronic kidney disease', 'PRILOSEC'): 733,
             ('Renal injury', 'PREVACID'): 719, ('Dyspnoea', 'XOLAIR'): 714}
        d1 = datetime.datetime(2019, 3, 1, 0)
        d2 = datetime.datetime(2019, 3, 20, 23)
        qc = {"receiptdate": {"$gt": d1, "$lt": d2}}
        r = self.qry.get_reaction_medicine_pairs(qc)
        r = {(i['_id']['reaction'], i['_id']['medicine']): i['abundance']
             for i in r}
        self.maxDiff = None
        self.assertDictEqual(r, e)

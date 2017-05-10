import unittest, os
from geneinfo.rnacentral_idmappings import mappingreader


class ReadersTestCase(unittest.TestCase):

    def test_rnacentral_idmapping_reader(self):
        d = os.path.dirname(os.path.abspath(__file__))
        infile = d + "/../data/rnacentral-6.0-id_mapping-first1000.tsv"
        l = [r for r in mappingreader(infile)]
        self.assertEqual(len(l), 342)
        r = l[0]
        print(r)
        self.assertEqual(r['_id'], 'URS0000000001')
        self.assertEqual(len(r['mappings']), 11)


if __name__ == '__main__':
    unittest.main()

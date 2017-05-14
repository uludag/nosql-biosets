import os
import unittest

from geneinfo.ensembl_regbuild import connectgffdb
from geneinfo.ensembl_regbuild import regregions
from geneinfo.ensembl_regbuild import tfs
from geneinfo.rnacentral_idmappings import mappingreader
from pubtator.index_pubtator_mappings import parse_pub2gene_lines


class ReadersTestCase(unittest.TestCase):

    d = os.path.dirname(os.path.abspath(__file__))

    def test_rnacentral_idmapping_reader(self):
        infile = self.d + "/../data/rnacentral-6.0-id_mapping-first1000.tsv"
        l = [r for r in mappingreader(infile)]
        self.assertEqual(len(l), 342)
        r = l[0]
        print(r)
        self.assertEqual(r['_id'], 'URS0000000001')
        self.assertEqual(len(r['mappings']), 11)

    def test_ensembl_regbuild_regions_reader(self):
        infile = self.d + "/../data/hg38.ensrb_features.r88.first100.gff"
        db = connectgffdb(infile)
        l = [r for r in regregions(db)]
        self.assertEqual(len(l), 100)

    def test_ensembl_regbuild_motifs_reader(self):
        infile = self.d + "/../data/hg38.ensrb_motiffeatures.r88.first1000.gff"
        db = connectgffdb(infile)
        l = [r for r in tfs(db)]
        self.assertEqual(len(l), 1000)

    def test_gene2pubtator_reader(self):
        infile = self.d + "/../data/gene2pubtator.sample"
        r = 0
        db = parse_pub2gene_lines(open(infile), r, 'gene2pub')
        l = [m for m in db]
        self.assertEqual(len(l), 1916)


if __name__ == '__main__':
    unittest.main()

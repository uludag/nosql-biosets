#!/usr/bin/env python
""" Tests with 'nosql-biosets' data readers """
import os
import unittest

from geneinfo.ensembl_regbuild import connectgffdb
from geneinfo.ensembl_regbuild import regregions
from geneinfo.ensembl_regbuild import tfs
from geneinfo.rnacentral_idmappings import mappingreader
from hmdb.index import parse_hmdb_xmlfile
from nosqlbiosets.kegg.index import read_and_index_kegg_xmltarfile
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
        with open(infile) as inf:
            db = parse_pub2gene_lines(inf, r, 'gene2pub')
            l = [m for m in db]
            self.assertEqual(len(l), 1916)

    def hmdb_reader_helper(self, _, entry):
        self.assertTrue('accession' in entry)
        self.nhmdbentries += 1
        return True

    def test_hmdb_reader(self):
        infile = self.d + "/../data/hmdb_csf_metabolites-first3.xml.gz"
        self.nhmdbentries = 0
        parse_hmdb_xmlfile(infile, self.hmdb_reader_helper)
        self.assertEqual(self.nhmdbentries, 3)
        infile = self.d + "/../data/hmdb_proteins-first10.xml.gz"
        self.nhmdbentries = 0
        parse_hmdb_xmlfile(infile, self.hmdb_reader_helper)
        self.assertEqual(self.nhmdbentries, 10)

    def kegg_xmlreader_helper(self, _, entry):
        self.assertTrue('pathway' in entry)
        self.nkeggentries += 1
        return True

    def test_kegg_xmltarfile_reader(self):
        infile = self.d + "/../data/kegg/xml/kgml/metabolic/" \
                          "organisms/hsa.tar.gz"
        self.nkeggentries = 0
        read_and_index_kegg_xmltarfile(infile, self.kegg_xmlreader_helper)
        self.assertEqual(self.nkeggentries, 92)


if __name__ == '__main__':
    unittest.main()

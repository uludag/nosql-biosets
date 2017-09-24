#!/usr/bin/env python
""" Tests with 'nosql-biosets' data readers """
import unittest

from geneinfo.ensembl_regbuild import connectgffdb
from geneinfo.ensembl_regbuild import regregions
from geneinfo.ensembl_regbuild import tfs
from geneinfo.rnacentral_idmappings import mappingreader
from hmdb.index import parse_hmdb_xmlfile
from nosqlbiosets.metanetx.index import *
from nosqlbiosets.kbase.index_modelseed import read_modelseed_datafile, \
    updatecompoundrecord, updatereactionrecord
from nosqlbiosets.kegg.index import read_and_index_kegg_xmltarfile
from nosqlbiosets.pathways.index_metabolic_networks \
    import read_and_index_model_files, sbml_to_cobra_json, \
    psamm_yaml_to_sbml, read_and_index_sbml_file
from nosqlbiosets.pubtator.index import parse_pub2gene_lines


class TestDataReaders(unittest.TestCase):

    d = os.path.dirname(os.path.abspath(__file__))

    def test_modelseed_creader(self):
        infile = self.d + "/data/modelseed/compounds.tsv"
        idlist = [r for r in read_modelseed_datafile(infile,
                                                     updatecompoundrecord)]
        self.assertGreaterEqual(len(idlist), 2000)
        r = idlist[0]
        print(r)
        self.assertEqual(r['_id'], 'cpd00001')

    def test_modelseed_rreader(self):
        infile = self.d + "/data/modelseed/reactions.tsv"
        idlist = [r for r in read_modelseed_datafile(infile,
                                                     updatereactionrecord)]
        self.assertGreaterEqual(len(idlist), 2000)
        r = idlist[0]
        print(r)
        self.assertEqual(r['_id'], 'rxn00001')

    def test_rnacentral_idmapping_reader(self):
        infile = self.d + "/../data/rnacentral-6.0-id_mapping-first1000.tsv"
        idlist = [r for r in mappingreader(infile)]
        self.assertEqual(len(idlist), 342)
        r = idlist[0]
        self.assertEqual(r['_id'], 'URS0000000001')
        self.assertEqual(len(r['mappings']), 11)

    def test_ensembl_regbuild_regions_reader(self):
        infile = self.d + "/../data/hg38.ensrb_features.r88.first100.gff"
        db = connectgffdb(infile)
        regions = [r for r in regregions(db)]
        self.assertEqual(len(regions), 100)

    def test_ensembl_regbuild_motifs_reader(self):
        infile = self.d + "/../data/hg38.ensrb_motiffeatures.r88.first1000.gff"
        db = connectgffdb(infile)
        tflist = [r for r in tfs(db)]
        self.assertEqual(len(tflist), 1000)

    def test_gene2pubtator_reader(self):
        infile = self.d + "/../data/gene2pubtator.sample"
        r = 0
        with open(infile) as inf:
            db = parse_pub2gene_lines(inf, r, 'gene2pub')
            mappinglist = [m for m in db]
            self.assertEqual(len(mappinglist), 1916)

    def hmdb_reader_helper(self, _, entry):
        self.assertTrue('accession' in entry)
        self.nhmdbentries += 1
        return True

    hmdbcsfmetabolites = d + "/../data/hmdb_csf_metabolites-first3.xml.gz"
    hmdbproteins = d + "/../data/hmdb_proteins-first10.xml.gz"

    @unittest.skipUnless(os.path.exists(hmdbproteins) and
                         os.path.exists(hmdbcsfmetabolites),
                         "Missing test files")
    def test_hmdb_reader(self):
        self.nhmdbentries = 0
        parse_hmdb_xmlfile(self.hmdbcsfmetabolites, self.hmdb_reader_helper)
        self.assertEqual(self.nhmdbentries, 3)
        self.nhmdbentries = 0
        parse_hmdb_xmlfile(self.hmdbproteins, self.hmdb_reader_helper)
        self.assertEqual(self.nhmdbentries, 10)

    compoundsxreffile = d + "/../metanetx/data/chem_xref-head.tsv"
    compoundsfile = d + "/../metanetx/data/chem_prop-head.tsv"

    @unittest.skipUnless(os.path.exists(compoundsfile) and
                         os.path.exists(compoundsxreffile),
                         "Missing test files")
    def test_metanetx_compound_reader(self):
        xrefsmap = getxrefs(self.compoundsxreffile, getcompoundxrefrecord)
        for r in read_metanetx_mappings(self.compoundsfile, getcompoundrecord,
                                        xrefsmap):
            if r['_id'] == 'MNXM1':
                self.assertEqual(r['inchikey'],
                                 'GPRLSGONYQIRFK-UHFFFAOYSA-N')
                break

    reactsxreffile = d + "/../metanetx/data/reac_xref-head.tsv"
    reactsfile = d + "/../metanetx/data/reac_prop-head.tsv"

    @unittest.skipUnless(os.path.exists(reactsfile) and
                         os.path.exists(reactsxreffile),
                         "Missing test files")
    def test_metanetx_reaction_reader(self):
        xrefsmap = getxrefs(self.reactsxreffile, getreactionxrefrecord)
        for r in read_metanetx_mappings(self.reactsfile, getreactionrecord,
                                        xrefsmap):
            if r['_id'] == 'MNXR94726':
                self.assertEqual(r['equation'],
                                 '1 MNXM3150@MNXD1 = 1 MNXM3150@MNXD2')
                break

    def kegg_xmlreader_helper(self, _, entry):
        self.assertTrue('name' in entry)
        self.nkeggentries += 1
        if self.nkeggentries < 4:
            return True
        else:
            return False

    keggtarfile = d + "/../data/kegg/xml/kgml/metabolic/organisms/hsa.tar.gz"

    @unittest.skipUnless(os.path.exists(keggtarfile),
                         "Missing test files")
    def test_kegg_xmltarfile_reader(self):
        self.nkeggentries = 0
        read_and_index_kegg_xmltarfile(self.keggtarfile,
                                       self.kegg_xmlreader_helper)
        self.assertEqual(self.nkeggentries, 4)

    psammmodelfiles = d + "/data/psamm/sbml/"

    def mock_sbml_indexer(self, _, r):
        self.assertTrue('metabolites' in r)
        self.assertGreater(len(r['metabolites']), 10)
        self.assertGreaterEqual(len(r['compartments']), 1)
        self.npsammentries += 1
        print("yaml files indexed = %d" % self.npsammentries)

    @unittest.skipUnless(os.path.exists(psammmodelfiles),
                         "Missing test files folder")
    def test_psamm_yamlfile_reader(self):
        for m in ["iIB711"]:
            yaml = self.psammmodelfiles + m + "/model.yaml"
            r = sbml_to_cobra_json(psamm_yaml_to_sbml(yaml))
            self.assertGreater(len(r['metabolites']), 10)
            self.assertGreaterEqual(len(r['compartments']), 1)
            self.assertGreater(len(r['genes']), 10)

    # Failing test for cobra.io.read_sbml_model:
    #     "compartments": { "C_c": null }  should be list
    @unittest.skipUnless(os.path.exists(psammmodelfiles),
                         "Missing test files folder")
    def test_psamm_yamlfile_reader(self):
        for m in ["S_coelicolor_fixed"]:
            yaml = self.psammmodelfiles + m + "/model.yaml"
            sbml = psamm_yaml_to_sbml(yaml)
            r = sbml_to_cobra_json(sbml)
            self.assertGreaterEqual(len(r['compartments']), 1)

    @unittest.skipUnless(os.path.exists(psammmodelfiles),
                         "Missing test files folder")
    def test_psamm_yamlfilesfolder_reader(self):
        self.npsammentries = 0
        read_and_index_model_files(self.psammmodelfiles,
                                   self.mock_sbml_indexer)
        self.assertGreater(self.npsammentries, 0)

    metanetxmodelfiles = d + "/data/metanetx/models/"

    @unittest.skipUnless(os.path.exists(metanetxmodelfiles),
                         "Missing test files folder")
    def test_sbmlfile(self):
        self.npsammentries = 0
        read_and_index_sbml_file(self.metanetxmodelfiles +
                                 "bigg_e_coli_core.COBRA-sbml3.xml",
                                 self.mock_sbml_indexer)
        self.assertGreater(self.npsammentries, 0)


if __name__ == '__main__':
    unittest.main()

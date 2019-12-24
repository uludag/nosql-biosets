#!/usr/bin/env python
"""Index ClinVar Variation Archive xml files, with Elasticsearch or MongoDB"""

from __future__ import print_function

import argparse
import traceback
from gzip import GzipFile
from multiprocessing.pool import ThreadPool

import xmltodict
from pymongo import IndexModel
from six import string_types

from nosqlbiosets.dbutils import DBconnection, dbargs
from nosqlbiosets.objutils import unifylistattribute, num

pool = ThreadPool(30)   # Threads for index calls, parsing is in the main thread
MAX_QUEUED_JOBS = 1400  # Maximum number of index jobs in queue


class Indexer(DBconnection):

    def __init__(self, dbtype, mdbdb, mdbcollection, esindex=None, host=None,
                 port=None, recreateindex=True):
        self.index = mdbdb if dbtype == 'MongoDB' else esindex
        self.dbtype = dbtype
        self.i = 1
        indxcfg = {  # for Elasticsearch
            "index.number_of_replicas": 0,
            "index.mapping.total_fields.limit": 14000,
            "index.refresh_interval": "6m"}
        super(Indexer, self).__init__(dbtype, self.index, host, port,
                                      mdbcollection=mdbcollection,
                                      es_indexsettings=indxcfg,
                                      recreateindex=recreateindex)
        if dbtype == "MongoDB":
            self.mcl = self.mdbi[mdbcollection]
            self.mcl.drop()

    # Read and Index entries in ClinVar xml file
    def parse_and_index_xmlfile(self, infile):
        infile = str(infile)
        print("Reading/indexing %s " % infile)
        if infile.endswith(".gz"):
            with GzipFile(infile) as inf:
                xmltodict.parse(inf, item_depth=2,
                                item_callback=self.index_clinvar_entry,
                                xml_attribs=True,
                                attr_prefix='')
        else:
            with open(infile, 'rb') as inf:
                xmltodict.parse(inf, item_depth=2,
                                item_callback=self.index_clinvar_entry,
                                xml_attribs=True,
                                attr_prefix='')

    def index_clinvar_entry(self, _, entry):
        def index():
            rtype = 'InterpretedRecord' if 'InterpretedRecord' in entry \
                else 'IncludedRecord'
            r = entry[rtype]
            _type = 'SimpleAllele' if 'SimpleAllele' in r \
                else 'Haplotype' if 'Haplotype' in r \
                else 'Genotype'
            docid = int(r[_type]['VariationID'])
            try:
                self.update_entry(entry)
                if self.dbtype == "Elasticsearch":
                    self.es.index(index=self.index,
                                  doc_type="_doc",
                                  ignore=409,
                                  filter_path=['hits.hits._id'],
                                  id=docid, body=entry)
                else:  # assume MongoDB
                    entry["_id"] = docid
                    self.mcl.insert_one(entry,
                                        bypass_document_validation=True)
                self.reportprogress(1000)
                return True
            except Exception as e:
                print("ERROR (docid=%d): %s" % (docid, e))
                print(traceback.format_exc())
                return False

        if pool._inqueue.qsize() > MAX_QUEUED_JOBS:
            from time import sleep
            print('sleeping 1 sec')
            sleep(1)
        pool.apply_async(index, [])
        return True

    def update_synonyms(self, sa):  # SimpleAllele
        if 'OtherNameList' in sa:
            unifylistattribute(sa, "OtherNameList", "Name",
                               renamelistto='otherNames')
            for i, name in enumerate(sa['otherNames']):
                if isinstance(name, string_types):
                    sa['otherNames'][i] = {'#text': name}

    # genotype or haplotype
    def update_genotype_haplotype(self, catype):
        if 'SimpleAllele' in catype:
            sa = catype['SimpleAllele']
            if isinstance(sa, list):
                for i in sa:
                    self.update_simpleallele(i)
            else:
                self.update_simpleallele(sa)

    def update_date(self, ca):
        if "DateLastEvaluated" in ca["Interpretation"] \
                and len(ca["Interpretation"]["DateLastEvaluated"]) > 10:
            ca["Interpretation"]["DateLastEvaluated"] = \
                ca["Interpretation"]["DateLastEvaluated"][:10]

    def update_comment(self, ca):
        if 'Comment' in ca:
            if not isinstance(ca['Comment'], list):
                ca['Comment'] = [ca['Comment']]
            for i, c in enumerate(ca['Comment']):
                if isinstance(c, string_types):
                    ca['Comment'][i] = {'#text': c}

    def update_simpleallele(self, sa):
        self.update_synonyms(sa)
        self.update_comment(sa)
        unifylistattribute(sa,
                           "MolecularConsequenceList",
                           "MolecularConsequence",
                           renamelistto='molecularConsequence')
        if 'molecularConsequence' in sa:
            self.update_comment(sa['molecularConsequence'])
        if 'FunctionalConsequence' in sa:
            self.update_comment(sa['FunctionalConsequence'])
        num(sa, 'AlleleID')
        num(sa, 'VariationID')

    def update_entry(self, entry):  # ClinVar Variation Archive entry
        if 'InterpretedRecord' in entry:
            ir = entry['InterpretedRecord']
            if 'SimpleAllele' in ir:
                self.update_simpleallele(ir['SimpleAllele'])
            if 'RCVList' in ir:
                unifylistattribute(ir, "RCVList", "RCVAccession",
                                   renamelistto='rcv')
                for i, rcvacc in enumerate(ir['rcv']):
                    if isinstance(rcvacc, string_types):
                        ir['rcv'][i] = {'#text': rcvacc}
                    else:
                        unifylistattribute(rcvacc, "InterpretedConditionList",
                                           "InterpretedCondition",
                                           renamelistto='interpretedCondition')
                        for j, ic in enumerate(rcvacc['interpretedCondition']):
                            if isinstance(ic, string_types):
                                rcvacc['interpretedCondition'][j] = {'#text': ic}
            unifylistattribute(ir, "ClinicalAssertionList", "ClinicalAssertion",
                               renamelistto='clinicalAssertion')
            for ca in ir['clinicalAssertion']:
                num(ca, 'ID')
                if "Interpretation" in ca:
                    self.update_date(ca)
                    self.update_comment(ca['Interpretation'])

                unifylistattribute(ca, "ObservedInList", "ObservedIn",
                                   renamelistto='observedIn')
                if 'SimpleAllele' in ca:
                    sa = ca['SimpleAllele']
                    self.update_simpleallele(sa)

                if 'Genotype' in ca:
                    self.update_genotype_haplotype(ca['Genotype'])
                if 'Haplotype' in ca:
                    self.update_genotype_haplotype(ca['Haplotype'])
                if 'TraitSet' in ca:
                    self.update_comment(ca['TraitSet'])

                self.update_comment(ca)
                for o in ca['observedIn']:
                    self.update_comment(o)
                    if not isinstance(o['Sample']['Species'], string_types):
                        o['Sample']['Species'] = o['Sample']['Species']['#text']
                    if 'TraitSet' in o:
                        self.update_comment(o['TraitSet'])
                    if 'ObservedData' in o:
                        self.update_comment(o['ObservedData'])
                    if 'Method' in o and 'ObsMethodAttribute' in o['Method']:
                        self.update_comment(o['Method']['ObsMethodAttribute'])


def mongodb_indices(mdb):
    print("\nProcessing text and field indices")
    index = IndexModel([
        ("InterpretedRecord.rcv.Title", "text"),
        ("InterpretedRecord.rcv.interpretedCondition.#text", "text"),
        ("InterpretedRecord.clinicalAssertion.observedIn.ObservedData."
         "Attribute.#text", "text"),
        ("InterpretedRecord.clinicalAssertion.Interpretation."
         "Comment.#text", "text"),
        ("InterpretedRecord.SimpleAllele.GeneList.Gene.FullName", "text"),
        ("InterpretedRecord.Interpretations.Interpretation."
         "ConditionList.TraitSet.Trait.AttributeSet.Attribute.#text", "text"),
        ("InterpretedRecord.Interpretations.Interpretation.Description", "text")
    ], name='text')
    mdb.create_indexes([index])
    indx_fields = [
        "RecordStatus",
        "InterpretedRecord.SimpleAllele.GeneList.Gene.Symbol",
        "InterpretedRecord.Interpretations.Interpretation."
        "ConditionList.TraitSet.Trait.Type",
        "InterpretedRecord.Interpretations.Interpretation."
        "ConditionList.TraitSet.Type",
        "InterpretedRecord.Interpretations.Interpretation.Type",
        "InterpretedRecord.Interpretations.Interpretation.Description",
        "InterpretedRecord.clinicalAssertion"
        ".observedIn.Method.MethodAttribute.Attribute.Type",
        "InterpretedRecord.clinicalAssertion"
        ".observedIn.Method.MethodAttribute.Attribute.#text"
    ]
    for field in indx_fields:
        mdb.create_index(field)


def main(infile, dbtype, mdbdb, mdbcollection, esindex, host=None, port=None,
         recreateindex=True):
    indxr = Indexer(dbtype, mdbdb, mdbcollection, esindex, host, port,
                    recreateindex=recreateindex)
    indxr.parse_and_index_xmlfile(infile)
    pool.close()
    pool.join()
    pool.terminate()
    print("\nCompleted reading and indexing the ClinVar entries")
    if dbtype == 'Elasticsearch':
        indxr.es.indices.refresh(index=esindex)
    else:
        mongodb_indices(indxr.mcl)


if __name__ == '__main__':
    args = argparse.ArgumentParser(
        description='Index ClinVar Variation Archive xml files,'
                    ' with Elasticsearch or MongoDB')
    args.add_argument('infile',
                      help='Input file name of ClinVar Variation Archive,'
                           ' compressed or uncompressed xml file')
    dbargs(args, mdbcollection='clinvarvariation', esindex='clinvarvariation')
    args = args.parse_args()
    main(args.infile, args.dbtype, args.mdbdb, args.mdbcollection, args.esindex,
         args.host, args.port)

#!/usr/bin/env python
"""Index ClinVar Variation Archive xml files, with Elasticsearch or MongoDB"""

from __future__ import print_function

import argparse
import os
import traceback
from gzip import GzipFile
from multiprocessing.pool import ThreadPool

import xmltodict
from pymongo import IndexModel
from six import string_types

from nosqlbiosets.dbutils import DBconnection
from nosqlbiosets.objutils import unifylistattribute

pool = ThreadPool(40)  # Threads for index calls, parsing is in the main thread
MAX_QUEUED_JOBS = 400  # Maximum number of index jobs in queue


class Indexer(DBconnection):

    def __init__(self, dbtype, db, collection, index=None, host=None, port=None):
        self.index = db if dbtype == 'MongoDB' else collection
        self.dbtype = dbtype
        self.i = 1
        indxcfg = {  # for Elasticsearch
            "index.number_of_replicas": 0,
            "index.mapping.total_fields.limit": 14000,
            "index.refresh_interval": "60m"}
        super(Indexer, self).__init__(dbtype, index, host, port,
                                      es_indexsettings=indxcfg,
                                      recreateindex=True)
        if db == "MongoDB":
            self.mcl = self.mdbi[collection]
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
            try:
                rtype = 'InterpretedRecord' if 'InterpretedRecord' in entry \
                    else 'IncludedRecord'
                r = entry[rtype]
                _type = 'SimpleAllele' if 'SimpleAllele' in r \
                    else 'Haplotype' if 'Haplotype' in r \
                    else 'Genotype'
                docid = int(r[_type]['VariationID'])
                self.update_entry(entry)
                if self.dbtype == "Elasticsearch":
                    self.es.index(index=self.index,
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
                if 'docid' in vars():
                    print("ERROR (docid=%d): %s" % (docid, e))
                print(traceback.format_exc())
                return False
        # return index(_attrs, entry)
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
                    self.update_synonyms(i)
            else:
                self.update_synonyms(sa)

    def update_date(self, ca):
        if "Interpretation" in ca \
                and "DateLastEvaluated" in ca["Interpretation"] \
                and len(ca["Interpretation"]["DateLastEvaluated"]) > 10:
            ca["Interpretation"]["DateLastEvaluated"] = \
                ca["Interpretation"]["DateLastEvaluated"][:10]

    def update_entry(self, entry):  # ClinVar Variation Archive entry
        if 'InterpretedRecord' in entry:
            ir = entry['InterpretedRecord']
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
                self.update_date(ca)
                unifylistattribute(ca, "ObservedInList", "ObservedIn",
                                   renamelistto='observedIn')
                if 'SimpleAllele' in ca:
                    self.update_synonyms(ca['SimpleAllele'])
                if 'Genotype' in ca:
                    self.update_genotype_haplotype(ca['Genotype'])
                if 'Haplotype' in ca:
                    self.update_genotype_haplotype(ca['Haplotype'])
                if 'Comment' in ca:
                    if not isinstance(ca['Comment'], list):
                        ca['Comment'] = [ca['Comment']]
                    for i, c in enumerate(ca['Comment']):
                        if isinstance(c, string_types):
                            ca['Comment'][i] = {'#text': c}
                for o in ca['observedIn']:
                    if not isinstance(o['Sample']['Species'], string_types):
                        o['Sample']['Species'] = o['Sample']['Species']['#text']


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
        "InterpretedRecord.Interpretations.Interpretation.Type",
        "InterpretedRecord.Interpretations.Interpretation.Description",
        "InterpretedRecord.clinicalAssertion"
        ".observedIn.Method.MethodAttribute.Attribute.Type"
    ]
    for field in indx_fields:
        mdb.create_index(field)


def main(infile, dbtype, db, collection, index, host=None, port=None):
    indxr = Indexer(dbtype, db, collection, index, host, port)
    indxr.parse_and_index_xmlfile(infile)
    pool.close()
    pool.join()
    print("\nCompleted reading and indexing the ClinVar entries")
    if dbtype == 'Elasticsearch':
        indxr.es.indices.refresh(index=collection)
    else:
        mongodb_indices(indxr.mcl)


if __name__ == '__main__':
    d = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description='Index ClinVar Variation Archive xml files,'
                    ' with Elasticsearch or MongoDB')
    parser.add_argument('infile',
                        help='Input file name for ClinVar Variation Archive'
                             ' compressed or uncompressed xml file')
    parser.add_argument('--mdbdb',
                        default="biosets",
                        help='Name of the MongoDB database')
    parser.add_argument('--mdbcollection', default='clinvarvariation',
                        help='Collection name for MongoDB')
    parser.add_argument('--esindex', default='clinvarvariation',
                        help='Index name for Elasticsearch')
    parser.add_argument('--host',
                        help='Elasticsearch or MongoDB server hostname')
    parser.add_argument('--port', type=int,
                        help="Elasticsearch or MongoDB server port number")
    parser.add_argument('--dbtype', default='Elasticsearch',
                        help="Database: 'Elasticsearch' or 'MongoDB'")
    args = parser.parse_args()
    main(args.infile, args.dbtype, args.mdbdb, args.mdbcollection, args.esindex,
         args.host, args.port)

#!/usr/bin/env python
"""Index HMDB protein/metabolite records with MongoDB and Elasticsearch,
 Download page: http://www.hmdb.ca/downloads """
# TODO: Elasticsearch default mappings may not be good enough
from __future__ import print_function

import argparse
import os
from gzip import GzipFile
from zipfile import ZipFile

import xmltodict
from pymongo import IndexModel

from nosqlbiosets.dbutils import DBconnection
from nosqlbiosets.objutils import unifylistattributes

DOCTYPE_METABOLITE = 'hmdbmetabolite'
DOCTYPE_PROTEIN = 'hmdbprotein'


# Read HMDB Metabolites/Proteins files, index using the function indexf
def parse_hmdb_xmlfile(infile, indexf):
    infile = str(infile)
    print("Reading/indexing %s " % infile)
    if infile.endswith(".gz"):
        with GzipFile(infile) as inf:
            xmltodict.parse(inf, item_depth=2, item_callback=indexf)
    elif infile.endswith(".zip"):
        with ZipFile(infile) as zipf:
            for fname in zipf.namelist():
                with zipf.open(fname) as inf:
                    xmltodict.parse(inf, item_depth=2,
                                    item_callback=indexf)
    else:
        with open(infile, 'rb', buffering=1000) as inf:
            xmltodict.parse(inf, item_depth=2, item_callback=indexf)
    print("\nCompleted")


class Indexer(DBconnection):

    def __init__(self, db, index, host, port, doctype):
        self.doctype = doctype
        self.index = index
        super(Indexer, self).__init__(db, index, host, port, recreateindex=True)
        if db == "MongoDB":
            self.mcl = self.mdbi[doctype]
            self.mcl.drop()

    # Tune entries for better data representation
    def tune(self, entry):
        list_attrs = ["synonyms", "pathways"]
        unifylistattributes(entry, list_attrs)
        if "taxonomy" in entry:
            list_attrs = ["alternative_parents", "substituents",
                          "external_descriptors"]
            unifylistattributes(entry["taxonomy"], list_attrs)

    # Index HMDB Metabolites/Proteins entry with Elasticsearch
    def es_index_hmdb_entry(self, _, entry):
        docid = entry['accession']
        self.tune(entry)
        if "taxonomy" in entry and entry['taxonomy'] is not None and\
                'molecular_framework' in entry['taxonomy']:
            del entry['taxonomy']['molecular_framework']
        if "cs_description" in entry:
            del entry['cs_description']
        try:
            self.es.index(index=self.index, doc_type=self.doctype,
                          id=docid, body=entry)
            self.reportprogress()
            r = True
        except Exception as e:
            print(e)
            r = False
        return r

    # Index HMDB Metabolites/Proteins entry with MongoDB
    def mongodb_index_hmdb_entry(self, _, entry):
        docid = entry['accession']
        self.tune(entry)
        spec = {"_id": docid}
        try:
            self.mcl.update(spec, entry, upsert=True)
            # TODO: replace update with insert
            self.reportprogress()
            r = True
        except Exception as e:
            print(e)
            r = False
        return r


def mongodb_indices(mdb, doctype):
    if doctype == DOCTYPE_METABOLITE:
        index = IndexModel([
            ("description", "text"), ("name", "text"),
            ("taxanomy.description", "text")])
        mdb.create_indexes([index])
        mdb.create_index("accession")
        mdb.create_index("protein_associations.protein.protein_accession")
        mdb.create_index("protein_associations.protein.gene_name")
    else:  # Proteins
        index = IndexModel([
            ("gene_name", "text"), ("general_function", "text"),
            ("specific_function", "text")])
        mdb.create_indexes([index])
        mdb.create_index("accession")
        mdb.create_index("metabolite_associations.metabolite.accession")
    return


def main(infile, index, doctype, db, host=None, port=None):
    if doctype is None:
        if 'protein' in infile:
            doctype = DOCTYPE_PROTEIN
        else:
            doctype = DOCTYPE_METABOLITE
    indxr = Indexer(db, index, host, port, doctype)
    if db == 'Elasticsearch':
        parse_hmdb_xmlfile(infile, indxr.es_index_hmdb_entry)
        indxr.es.indices.refresh(index=index)
    else:
        parse_hmdb_xmlfile(infile, indxr.mongodb_index_hmdb_entry)
        mongodb_indices(indxr.mcl, doctype)


if __name__ == '__main__':
    d = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description='Index HMDB proteins/metabolites datasets,'
                    ' with Elasticsearch or MongoDB')
    parser.add_argument('-infile', '--infile',
                        required=True,
                        help='Input file name')
    parser.add_argument('--index',
                        default="hmdb",
                        help='Name of the Elasticsearch index or MongoDB db')
    parser.add_argument('--doctype',
                        help='Document type (hmdbprotein or hmdbmetabolite)')
    parser.add_argument('--host',
                        help='Elasticsearch or MongoDB server hostname')
    parser.add_argument('--port',
                        help="Elasticsearch or MongoDB server port number")
    parser.add_argument('--db', default='Elasticsearch',
                        help="Database: 'Elasticsearch' or 'MongoDB'")
    args = parser.parse_args()
    main(args.infile, args.index, args.doctype, args.db, args.host, args.port)

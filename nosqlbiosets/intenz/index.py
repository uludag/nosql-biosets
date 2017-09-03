#!/usr/bin/env python
"""Index IntEnz xml files, with Elasticsearch or MongoDB"""
# TODO:
# - Threaded index calls

from __future__ import print_function

import argparse
import logging
import os
import traceback
from multiprocessing.pool import ThreadPool

import xmltodict
from six import string_types

from nosqlbiosets.dbutils import DBconnection

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)
pool = ThreadPool(10)
DOCTYPE = 'intenz'  # Default document type for IntEnz entries


class Indexer(DBconnection):

    def __init__(self, db, index, host, port, doctype):
        self.doctype = doctype
        self.index = index
        self.db = db
        indxcfg = {
            "index.number_of_replicas": 0,
            "index.number_of_shards": 5,
            "index.refresh_interval": "10s"}
        super(Indexer, self).__init__(db, index, host, port,
                                      es_indexsettings=indxcfg,
                                      recreateindex=False)
        if db != "Elasticsearch":
            self.mcl = self.mdbi[doctype]

    # Read IntEnz xml file, index using the function indexf
    def parse_intenz_xmlfiles(self, infile):
        infile = str(infile)
        print("Reading/indexing %s " % infile)
        if not infile.endswith(".xml"):
            print("Input file should be an .xml file")
        else:
            with open(infile, 'rb', buffering=1000) as inf:
                xmltodict.parse(inf, item_depth=5,
                                item_callback=self.index_intenz_entry,
                                attr_prefix='')
        print("\nCompleted")

    def index_intenz_entry(self, _, entry):
        def index():
            docid = entry['ec'][3:]
            logger.debug(docid)
            try:
                if self.db == "Elasticsearch":
                    self.es.index(index=self.index, doc_type=self.doctype,
                                  op_type='create', ignore=409,
                                  filter_path=['hits.hits._id'],
                                  id=docid, body=entry)
                else:  # assume MongoDB
                    spec = {"_id": docid}
                    self.mcl.update(spec, entry, upsert=True)
            except Exception as e:
                print("ERROR: %s" % e)
                print(traceback.format_exc())
                exit(-1)
            self.reportprogress(100)

        # pool.apply_async(index, ())
        if not isinstance(entry, string_types):
            index()
        return True


def mongodb_textindex(mdb):
    index = [
        ("comments.comment.#text", "text"), ("synonyms.synonym.#text", "text")]
    mdb.create_index(index, name="text")


def main(infile, index, doctype, db, host, port):
    indxr = Indexer(db, index, host, port, doctype)
    indxr.parse_intenz_xmlfiles(infile)
    if db == 'Elasticsearch':
        indxr.es.indices.refresh(index=index)
    else:
        mongodb_textindex(indxr.mcl)


if __name__ == '__main__':
    d = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description='Index IntEnz xml files,'
                    ' with Elasticsearch or MongoDB')
    parser.add_argument('-infile', '--infile',
                        default=d+"/../../data/intenz/ASCII/intenz.xml",
                        help='Input file name')
    parser.add_argument('--index',
                        default="biosets",
                        help='Name of the Elasticsearch index'
                             ' or MongoDB database')
    parser.add_argument('--doctype', default=DOCTYPE,
                        help='Document type name')
    parser.add_argument('--host',
                        help='Elasticsearch or MongoDB server hostname')
    parser.add_argument('--port',
                        help="Elasticsearch or MongoDB server port number")
    parser.add_argument('--db', default='Elasticsearch',
                        help="Database: 'Elasticsearch' or 'MongoDB'")
    args = parser.parse_args()
    main(args.infile, args.index, args.doctype, args.db, args.host, args.port)

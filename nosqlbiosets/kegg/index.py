#!/usr/bin/env python
"""Index KEGG xml files """
from __future__ import print_function

import argparse
import json
import os
import sys
import tarfile

import xmltodict
from pymongo import IndexModel

from nosqlbiosets.dbutils import DBconnection


# Read given KEGG pathway xml/tar file,
# index using the index function specified
def read_and_index_kegg_xmltarfile(infile, indexf):
    print("\nProcessing tar file: %s " % infile)
    i = 0
    tar = tarfile.open(infile, 'r:gz')
    for member in tar:
        f = tar.extractfile(member)
        if f is None:
            continue  # if the tarfile entry is a folder then skip
        r = xmltodict.parse(f, attr_prefix='')
        if not indexf(1, r['pathway']):
            break
        print(".", end='')
        sys.stdout.flush()
    return i


# Read and index KEGG Pathway files (possibly in a folder)
def read_and_index_kegg_xmlfiles(infile, indexf):
    if os.path.isdir(infile):
        for child in os.listdir(infile):
            c = os.path.join(infile, child)
            read_and_index_kegg_xmlfile(c, indexf)
    else:
        read_and_index_kegg_xmlfile(infile, indexf)


# Read KEGG Pathway files, index using the function indexf
def read_and_index_kegg_xmlfile(infile, indexf):
    infile = str(infile)
    print("Reading/indexing %s " % infile)
    if infile.endswith(".tar.gz"):
        read_and_index_kegg_xmltarfile(infile, indexf)
    elif infile.endswith(".xml"):
        with open(infile, 'rb', buffering=1000) as inf:
            r = xmltodict.parse(inf, attr_prefix='')
            indexf(1, r['pathway'])
    else:
        print("only .xml and .tar.gz files are read and indexed")
    print("\nCompleted")


class Indexer(DBconnection):

    def __init__(self, db, index, host, port, doctype):
        self.index = index
        self.doctype = doctype
        super(Indexer, self).__init__(db, index, host, port)
        if db != "Elasticsearch":
            self.mcl = self.mdbi[doctype]

    # Prepare reaction objects for indexing
    @staticmethod
    def update_reaction(r):
        r['id'] = int(r['id'])
        for c in ['substrate', 'compound']:
            if c in r:
                if isinstance(r[c], list):
                    for e in r[c]:
                        e['id'] = int(e['id'])
                else:
                    r[c]['id'] = int(r[c]['id'])

    # Prepare entry for indexing
    def update_entry(self, entry):
        # TODO: 'relation' and 'graphics' fields are deleted
        # until we better understand the data
        if 'relation' in entry or hasattr(entry, 'relation'):
            del (entry['relation'])
        for e in entry['entry']:
            e['id'] = int(e['id'])
            del(e['graphics'])
            if 'link' in e:
                del(e['link'])
        if 'reaction' in entry:
            if isinstance(entry['reaction'], dict):
                self.update_reaction(entry['reaction'])
            else:
                for r in entry['reaction']:
                    self.update_reaction(r)

    # Index KEGG Pathway entry with Elasticsearch
    def es_index_kegg_entry(self, _, entry):
        print(".", end='')
        sys.stdout.flush()
        docid = entry['name']
        self.update_entry(entry)
        try:
            self.es.index(index=self.index, doc_type=self.doctype,
                          id=docid, body=json.dumps(entry))
            return True
        except Exception as e:
            print(e)
        return False

    # Index KEGG Pathway entry with MongoDB
    def mongodb_index_kegg_entry(self, _, entry):
        print(".", end='')
        sys.stdout.flush()
        docid = entry['name']
        spec = {"_id": docid}
        self.update_entry(entry)
        try:
            self.mcl.update(spec, entry, upsert=True)
            return True
        except Exception as e:
            print(e)
        return False


def mongodb_textindex(mdb):
    index = IndexModel([("title", "text")])
    mdb.create_indexes([index])
    return


def main(infile, index, doctype, db, host, port):
    indxr = Indexer(db, index, host, port, doctype)
    if db == 'Elasticsearch':
        read_and_index_kegg_xmlfiles(infile, indxr.es_index_kegg_entry)
        indxr.es.indices.refresh(index=index)
    else:
        read_and_index_kegg_xmlfiles(infile, indxr.mongodb_index_kegg_entry)
        mongodb_textindex(indxr.mcl)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Index KEGG pathway xml records,'
                    ' with Elasticsearch or MongoDB')
    parser.add_argument('-infile', '--infile',
                        help='Individual KEGG xml file or archive of them, '
                             'such as hsa01210.xml or hsa.tar.gz')
    parser.add_argument('--index',
                        default="nosqlbiosets",
                        help='Name of the Elasticsearch index'
                             ' or MongoDB database')
    parser.add_argument('--doctype',
                        default='kegg_pathway',
                        help='Name for the Elasticsearch document types or'
                             'MongoDB collection')
    parser.add_argument('--host',
                        help='Elasticsearch or MongoDB server hostname')
    parser.add_argument('--port',
                        help="Elasticsearch or MongoDB server port number")
    parser.add_argument('--db', default='Elasticsearch',
                        help="Database: 'Elasticsearch' or 'MongoDB'")
    args = parser.parse_args()
    main(args.infile, args.index, args.doctype, args.db, args.host, args.port)

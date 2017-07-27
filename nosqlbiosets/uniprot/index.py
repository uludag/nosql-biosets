#!/usr/bin/env python3
"""Index UniProt uniprot_sprot.xml.gz files, with Elasticsearch or MongoDB"""
# TODO:
# - Proper mapping for all attributes
#   (unhandled attributes are deleted for now)
# - Python2 support: type str vs unicode for text attributes

from __future__ import print_function

import argparse
import json
import logging
import os
import traceback
from gzip import GzipFile

import xmltodict
from pymongo import IndexModel

from nosqlbiosets.dbutils import DBconnection

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)


# Read UniProt xml files, index using the function indexf
def parse_uniprot_xmlfiles(infile, indexf):
    infile = str(infile)
    print("Reading/indexing %s " % infile)
    if infile.endswith(".gz"):
        with GzipFile(infile) as inf:
            xmltodict.parse(inf, item_depth=2, item_callback=indexf,
                            attr_prefix='')
    else:
        with open(infile, 'rb', buffering=1000) as inf:
            xmltodict.parse(inf, item_depth=2, item_callback=indexf,
                            attr_prefix='')
    print("\nCompleted")


class Indexer(DBconnection):

    def __init__(self, db, index, host, port, doctype):
        self.doctype = doctype
        self.index = index
        super(Indexer, self).__init__(db, index, host, port,
                                      recreateindex=False)
        if db != "Elasticsearch":
            self.mcl = self.mdbi[doctype]

    # Prepare 'comments' for indexing
    # Sample err msg: failed to parse [comment.absorption.text]
    @staticmethod
    def updatecomment(c):
        al = ['isoform', 'subcellularLocation', 'kinetics', 'phDependence',
              'temperatureDependence', 'redoxPotential', 'absorption']
        for a in al:
            if a in c:
                del c[a]
        if hasattr(c, 'location'):
            c['location']['begin']['position'] =\
                int(c['location']['begin']['position'])
            c['location']['end']['position'] =\
                int(c['location']['end']['position'])

    # Prepare 'features' for indexing
    def updatefeatures(self, e):
        if isinstance(e['feature'], list):
            for f in e['feature']:
                if 'location' in f:
                    self.updatelocation(f)
        else:
            self.updatelocation(e['feature'])

    # Prepare 'locations' for indexing
    # @staticmethod
    def updatelocation(self, obj):
        if 'location' in obj:
            l = obj['location']
            if isinstance(l, list):
                for i in l:
                    self.updateposition(i)
            else:
                self.updateposition(l)

    # Prepare 'positions' for indexing
    @staticmethod
    def updateposition(l):
        if 'position' in l:
            l['position']['position'] = int(l['position']['position'])
        else:
            for i in ['begin', 'end']:
                if 'position' in l[i]:
                    l[i]['position'] = int(l[i]['position'])

    # Update UniProt entries 'protein' field
    # Sample err msg:failed to parse [protein.domain.recommendedName.fullName
    @staticmethod
    def updateprotein(e):
        al = ['recommendedName', 'alternativeName', 'allergenName',
              'domain', 'component', 'cdAntigenName', 'innName']
        for a in al:
            if hasattr(e['protein'], a):
                if isinstance(e['protein'][a], str):
                    text = {'#text': e['protein'][a]}
                    e['protein'][a] = text
            elif a in e['protein']:
                del e['protein'][a]

    @staticmethod
    def updatesequence(s):
        del(s['#text'])
        s['mass'] = int(s['mass'])
        s['length'] = int(s['length'])
        s['version'] = int(s['version'])

    # Prepare UniProt entry for indexing
    def update_entry(self, entry):
        if 'comment' in entry:
            if isinstance(entry['comment'], list):
                for c in entry['comment']:
                    self.updatecomment(c)
                    if 'text' in c:
                        if isinstance(c['text'], str):
                            c['text'] = {'#text': c['text']}
                    self.updatelocation(c)
            else:
                entry['comment'] = None
        self.updateprotein(entry)
        if 'reference' in entry:
            if isinstance(entry['reference'], list):
                for r in entry['reference']:
                    r['source'] = None
            else:
                entry['reference']['source'] = None
        self.updatefeatures(entry)
        self.updatesequence(entry['sequence'])

    # Index UniProt entry with Elasticsearch
    def es_index_uniprot_entry(self, _, entry):
        docid = entry['name']
        if self.es.exists(index=self.index, doc_type=self.doctype, id=docid):
            return True
        try:
            logger.debug(docid)
            print("docid:%s  n=%d" % (docid, len(json.dumps(entry))))
            self.update_entry(entry)
            self.es.index(index=self.index, doc_type=self.doctype,
                          id=docid, body=json.dumps(entry))
            self.reportprogress()
            r = True
        except Exception as e:
            r = False
            print("ERROR: %s" % e)
            print(traceback.format_exc())
            exit(-1)
            # raise e
        return r

    # Index UniProt entry with MongoDB
    def mongodb_index_uniprot_entry(self, _, entry):
        docid = entry['name']
        spec = {"_id": docid}
        print("docid:%s  n=%d" % (docid, len(json.dumps(entry))))
        try:
            self.update_entry(entry)
            self.mcl.update(spec, entry, upsert=True)
            self.reportprogress()
            r = True
        except Exception as e:
            print(e)
            r = False
        return r


def mongodb_textindex(mdb, doctype):
    if doctype == 'metabolite':
        index = IndexModel([
            ("description", "text"), ("name", "text"),
            ("taxanomy.description", "text")])
        mdb.create_indexes([index])
    return


def main(infile, index, doctype, db, host, port):
    indxr = Indexer(db, index, host, port, doctype)
    if db == 'Elasticsearch':
        parse_uniprot_xmlfiles(infile, indxr.es_index_uniprot_entry)
        indxr.es.indices.refresh(index=index)
    else:
        parse_uniprot_xmlfiles(infile, indxr.mongodb_index_uniprot_entry)
        mongodb_textindex(indxr.mcl, doctype)


if __name__ == '__main__':
    d = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description='Index UniProt xml files,'
                    ' with Elasticsearch or MongoDB')
    parser.add_argument('-infile', '--infile',
                        default=d+"/../../data/uniprot_sprot.xml.gz",
                        help='Input file name')
    parser.add_argument('--index',
                        default="nosqlbiosets",
                        help='Name of the Elasticsearch index'
                             ' or MongoDB database')
    parser.add_argument('--doctype', default='protein',
                        help='Document type name')
    parser.add_argument('--host',
                        help='Elasticsearch or MongoDB server hostname')
    parser.add_argument('--port',
                        help="Elasticsearch or MongoDB server port number")
    parser.add_argument('--db', default='Elasticsearch',
                        help="Database: 'Elasticsearch' or 'MongoDB'")
    args = parser.parse_args()
    main(args.infile, args.index, args.doctype, args.db, args.host, args.port)

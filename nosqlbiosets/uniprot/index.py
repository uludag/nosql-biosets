#!/usr/bin/env python
"""Index UniProt uniprot_sprot.xml.gz files, with Elasticsearch or MongoDB"""
# TODO:
# - Proper mapping for all attributes
#   (unhandled attributes are deleted for now)

import argparse
import traceback
from gzip import GzipFile
from multiprocessing.pool import ThreadPool

import xmltodict
from pymongo import IndexModel
from six import string_types

from nosqlbiosets.dbutils import DBconnection, dbargs

pool = ThreadPool(14)   # Threads for index calls, parsing is in the main thread
MAX_QUEUED_JOBS = 1400  # Maximum number of index jobs in queue
MDBCOLLECTION = 'uniprot'


class Indexer(DBconnection):

    def __init__(self, db, esindex, mdbdb, mdbcollection=MDBCOLLECTION,
                 host=None, port=None,
                 recreateindex=True):
        self.index = esindex if db == "Elasticsearch" else mdbdb
        self.db = db
        indxcfg = {  # for Elasticsearch
            "index.number_of_replicas": 0,
            "index.number_of_shards": 5,
            "index.refresh_interval": "1m"}
        super(Indexer, self).__init__(db, self.index, host, port,
                                      mdbcollection=mdbcollection,
                                      es_indexsettings=indxcfg,
                                      recreateindex=recreateindex)
        if db == "MongoDB":
            self.mcl = self.mdbi[mdbcollection]
            self.mcl.drop()

    # Read and Index entries in UniProt xml file
    def parse_uniprot_xmlfiles(self, infile):
        infile = str(infile)
        print("Reading/indexing %s " % infile)
        if infile.endswith(".gz"):
            with GzipFile(infile) as inf:
                xmltodict.parse(inf, item_depth=2,
                                item_callback=self.index_uniprot_entry,
                                attr_prefix='')
        else:
            with open(infile, 'rb') as inf:
                xmltodict.parse(inf, item_depth=2,
                                item_callback=self.index_uniprot_entry,
                                attr_prefix='')
        print("\nCompleted")

    def index_uniprot_entry(self, _, entry):
        def index():
            try:
                self.update_entry(entry)
                docid = entry['name']
                if self.db == "Elasticsearch":
                    self.es.index(index=self.index,
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
            self.reportprogress(1000)
        if isinstance(entry, string_types):  # Assume <copyright> notice
            print("\nUniProt copyright notice: %s " % entry.strip())
        else:
            if pool._inqueue.qsize() > MAX_QUEUED_JOBS:
                from time import sleep
                print('sleeping 1 sec')
                sleep(1)
            pool.apply_async(index, ())
        return True

    # Prepare 'comments' for indexing
    # Sample err msg: failed to parse [comment.absorption.text]
    @staticmethod
    def updatecomment(c):
        if 'text' in c:
            if isinstance(c['text'], string_types):
                c['text'] = {'#text': c['text']}
        # Following attributes are deleted for various reasons
        # We want to implement support for all UniProt attributes
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
    def updatelocation(self, obj):
        if 'location' in obj:
            loc = obj['location']
            if isinstance(loc, list):
                for i in loc:
                    self.updateposition(i)
            else:
                self.updateposition(loc)

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
              'component', 'cdAntigenName', 'innName']
        
        if 'domain' in e['protein']:
            del e['protein']['domain']

        for a in al:
            if hasattr(e['protein'], a):
                if isinstance(e['protein'][a], string_types):
                    text = {'#text': e['protein'][a]}
                    e['protein'][a] = text
            elif a in e['protein']:
                def getname(name):
                    if isinstance(name, string_types):
                        return name
                    if 'fullName' in name:
                        return name['fullName']['#text'] \
                                if '#text' in name['fullName'] else \
                                name['fullName']

                if not isinstance(e['protein'][a], list):
                    e['protein'][a] = getname(e['protein'][a])
                else:
                    e['protein'][a] = getname(e['protein'][a][0])

    @staticmethod
    def updatesequence(s):
        del(s['#text'])
        s['mass'] = int(s['mass'])
        s['length'] = int(s['length'])
        s['version'] = int(s['version'])

    # Prepare UniProt entry for indexing
    # organism.name should always be list?
    # organism.lineage.taxon should be list
    # all has scientific name, half has common name, 1/10th has synonym(s?)
    def update_entry(self, entry):
        # Make sure type of 'gene' attr is list
        if 'gene' in entry:
            if not isinstance(entry['gene'], list):
                entry['gene'] = [entry['gene']]
            # Make sure type of 'gene.name' attribute is list
            for gene in entry['gene']:
                if not isinstance(gene['name'], list):
                    gene['name'] = [gene['name']]
        if 'comment' in entry:
            if isinstance(entry['comment'], list):
                for c in entry['comment']:
                    self.updatecomment(c)
                    self.updatelocation(c)
            else:
                self.updatecomment(entry['comment'])
                self.updatelocation(entry['comment'])
                entry['comment'] = [entry['comment']]
        self.updateprotein(entry)
        if 'reference' in entry:
            if isinstance(entry['reference'], list):
                for r in entry['reference']:
                    if 'source' in r:
                        del r['source']
            elif 'source' in entry['reference']:
                del entry['reference']['source']
        self.updatefeatures(entry)
        self.updatesequence(entry['sequence'])


def mongodb_indices(mdb):
    index = IndexModel([
        ("comment.text.#text", "text"),
        ("feature.description", "text"),
        ("keyword.#text", "text"),
        ("reference.citation.title", "text")
    ], name='text')
    mdb.create_indexes([index])
    indx_fields = ["accession",
                   "dbReference.id", "dbReference.type", "dbReference.property",
                   "feature.type",
                   'comment.type', "gene.name.type",
                   "gene.name.#text",
                   "name",
                   "organism.dbReference.id",
                   "organism.lineage.taxon",
                   "organism.name.#text"]
    for field in indx_fields:
        mdb.create_index(field)


def main(infile, esindex, mdbdb, mdbcollection, db, host=None, port=None,
         recreateindex=True):
    indxr = Indexer(db, esindex, mdbdb, mdbcollection, host, port,
                    recreateindex=recreateindex)
    indxr.parse_uniprot_xmlfiles(infile)
    pool.close()
    pool.join()
    pool.terminate()
    if db == 'Elasticsearch':
        indxr.es.indices.refresh(index=esindex)
    else:
        mongodb_indices(indxr.mcl)


if __name__ == '__main__':
    args = argparse.ArgumentParser(
        description='Index UniProt xml files,'
                    ' with Elasticsearch or MongoDB')
    args.add_argument('infile',
                      help='Input file name for UniProt Swiss-Prot compressed'
                           ' xml dataset')
    dbargs(args)
    args = args.parse_args()
    main(args.infile, args.esindex, args.mdbdb, args.mdbcollection,
         args.dbtype, args.host, args.port)

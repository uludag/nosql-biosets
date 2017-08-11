#!/usr/bin/env python
"""Index metabolic network files, current/initial version is limited
   to PSAMM yaml files """
from __future__ import print_function

import argparse
import json
import logging
import os
import sys

import cobra
from nosqlbiosets.dbutils import DBconnection
from psamm.datasource import native, sbml
from pymongo import IndexModel

logger = logging.getLogger(__name__)


def psamm_yaml_to_cobra_json(inf):
    reader = native.ModelReader.reader_from_path(inf)
    print(reader.name)
    m = reader.create_model()
    writer = sbml.SBMLWriter()
    tempsbml = "temp.sbml"
    try:
        writer.write_model(tempsbml, m)
    except TypeError as e:
        print(e)
        return None
    c = cobra.io.read_sbml_model(tempsbml)
    tempjson = "temp.json"
    cobra.io.save_json_model(c, tempjson)
    r = json.load(open(tempjson))
    os.unlink(tempsbml)
    os.unlink(tempjson)
    return r


# Read and index PSAMM yaml files (possibly in a folder)
def read_and_index_psamm_yamlfiles(infile, indexf):
    if os.path.isdir(infile) and not os.path.exists(
            os.path.join(infile, 'model.yaml')):
        for child in os.listdir(infile):
            c = os.path.join(infile, child)
            read_and_index_psamm_yamlfile(c, indexf)
    else:
        read_and_index_psamm_yamlfile(infile, indexf)


# Read PSAMM yaml files, index using the function indexf
def read_and_index_psamm_yamlfile(infile, indexf):
    infile = os.path.join(infile, "model.yaml")
    print("Reading/indexing %s " % infile)
    if os.path.exists(infile):
        r = psamm_yaml_to_cobra_json(infile)
        if r is not None:
            for react in r['reactions']:
                ml = [{"id": mid, "st": react['metabolites'][mid]}
                      for mid in react['metabolites']]
                react['metabolites'] = ml
            indexf(1, r)
    else:
        print("No model.yaml file found")
    print("\nCompleted")


class Indexer(DBconnection):

    def __init__(self, db, index, host, port, doctype):
        self.index = index
        self.doctype = doctype
        es_indexsettings = {
            "index.mapping.total_fields.limit": 8000,
            "number_of_replicas": 0}
        super(Indexer, self).__init__(db, index, host, port,
                                      recreateindex=True,
                                      es_indexsettings=es_indexsettings)
        if db != "Elasticsearch":
            self.mcl = self.mdbi[doctype]

    # Index metabolic network with Elasticsearch
    def es_index_sbml(self, _, entry):
        print(".", end='')
        sys.stdout.flush()
        docid = entry['name']
        try:
            self.es.index(index=self.index, doc_type=self.doctype,
                          id=docid, body=json.dumps(entry))
            return True
        except Exception as e:
            print(e)
        return False

    # Index metabolic network with MongoDB
    def mongodb_index_sbml(self, _, entry):
        print(".", end='')
        sys.stdout.flush()
        docid = entry['name']
        spec = {"_id": docid}
        try:
            self.mcl.update(spec, entry, upsert=True)
            return True
        except Exception as e:
            print(e)
        return False


def mongodb_textindex(mdb):
    index = IndexModel([("name", "text")])
    mdb.create_indexes([index])
    return


def main(infile, index, doctype, db, host, port):
    indxr = Indexer(db, index, host, port, doctype)
    if db == 'Elasticsearch':
        read_and_index_psamm_yamlfiles(infile, indxr.es_index_sbml)
        indxr.es.indices.refresh(index=index)
    else:
        read_and_index_psamm_yamlfiles(infile, indxr.mongodb_index_sbml)
        mongodb_textindex(indxr.mcl)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Index PSAMM metabolic network yaml records,'
                    ' with Elasticsearch or MongoDB')
    parser.add_argument('-infile', '--infile',
                        help='Folder for individual PSAMM metabolic network or'
                             ' folder of metabolic networks folders')
    parser.add_argument('--index',
                        default="biotests",
                        help='Name of the Elasticsearch index'
                             ' or MongoDB database')
    parser.add_argument('--doctype',
                        default='psamm_metabolic_network',
                        help='Name for the Elasticsearch document type or '
                             'MongoDB collection')
    parser.add_argument('--host',
                        help='Elasticsearch or MongoDB server hostname')
    parser.add_argument('--port',
                        help="Elasticsearch or MongoDB server port number")
    parser.add_argument('--db', default='Elasticsearch',
                        help="Database: 'Elasticsearch' or 'MongoDB'")
    args = parser.parse_args()
    main(args.infile, args.index, args.doctype, args.db, args.host, args.port)

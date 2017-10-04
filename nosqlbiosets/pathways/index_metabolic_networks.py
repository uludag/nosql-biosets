#!/usr/bin/env python
"""Index metabolic network files, current/initial version is limited
   to SBML files and psamm-model-collection project yaml files """
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import json
import logging
import os

import cobra
from psamm.datasource import native, sbml
from psamm.expression.boolean import ParseError
from pymongo import IndexModel

from nosqlbiosets.dbutils import DBconnection

logger = logging.getLogger(__name__)
INDEX = 'biosets'
DOCTYPE = 'metabolic_network'


def psamm_yaml_to_sbml(inf):
    reader = native.ModelReader.reader_from_path(inf)
    print(reader.name)
    m = reader.create_model()
    writer = sbml.SBMLWriter(cobra_flux_bounds=True)
    tempsbml = inf + ".sbml"
    print(tempsbml)
    try:
        writer.write_model(tempsbml, m, pretty=True)
        return tempsbml
    except TypeError as e:
        print("Type error while saving %s in SBML: %s" % (inf, e))
    except ParseError as e:
        print("Parser error while saving %s in SBML: %s" % (inf, e))
    return None


def sbml_to_cobra_json(inf):
    c = cobra.io.read_sbml_model(inf)
    r = cobra.io.model_to_dict(c)
    return r


class SBMLIndexer(DBconnection):

    def __init__(self, db, index=INDEX, doctype=DOCTYPE, host=None, port=None):
        self.index = index
        self.doctype = doctype
        self.db = db
        es_indexsettings = {
            "index.mapping.total_fields.limit": 8000,
            "number_of_replicas": 0}
        super(SBMLIndexer, self).__init__(db, index, host, port,
                                          recreateindex=True,
                                          es_indexsettings=es_indexsettings)
        if db != "Elasticsearch":
            print(doctype)
            self.mcl = self.mdbi[doctype]

    # Read and index metabolic network files, PSAMM yaml or sbml
    def read_and_index_model_files(self, infile):
        if os.path.isdir(infile):
            for child in os.listdir(infile):
                c = os.path.join(infile, child)
                if os.path.isdir(c) and os.path.exists(
                        os.path.join(c, "model.yaml")):
                    c = os.path.join(c, "model.yaml")
                self.read_and_index_model_file(c)
        else:
            self.read_and_index_model_file(infile)
        if self.db == 'Elasticsearch':
            self.es.indices.refresh(index=self.index)
        else:
            index = IndexModel([("name", "text")])
            self.mdbi[self.doctype].create_indexes([index])

    # Read PSAMM yaml or SBML file, index using the database selected earlier
    def read_and_index_model_file(self, infile):
        print("Reading/indexing %s " % infile)
        if not os.path.exists(infile):
            print("Input file not found")
            raise FileNotFoundError(infile)
        if infile.endswith(".yaml"):
            try:
                infile_ = psamm_yaml_to_sbml(infile)
                if infile_ is not None:
                    self.read_and_index_sbml_file(infile_)
                else:
                    print("Unable to process PSAMM yaml file: %s" % infile)
            except Exception as e:
                print("Error while processing PSAMM yaml file: %s, %s" %
                      (infile, e))
        elif infile.endswith(".xml") or infile.endswith(".sbml"):
            try:
                self.read_and_index_sbml_file(infile)
            except Exception as e:
                print("Error while processing SBML file: %s, %s" %
                      (infile, e))
        else:
            print(
                "Only .xml, .sbml (for SBML) and .yaml (for PSAMM)"
                " files are supported")
            return

    # Read sbml file, index using the function indexf
    def read_and_index_sbml_file(self, infile):
        if os.path.exists(infile):
            r = sbml_to_cobra_json(infile)
            # Changes to COBRAby json, see readme file in this folder
            if r is not None:
                for react in r['reactions']:
                    ml = [{"id": mid, "st": react['metabolites'][mid]}
                          for mid in react['metabolites']]
                    react['metabolites'] = ml
                if r['id'] is None:
                    del (r['id'])
                if self.db == "Elasticsearch":
                    self.es_index_sbml(1, r)
                else:  # "MongoDB"
                    self.mongodb_index_sbml(1, r)
        else:
            print("SBML file not found")

    # Index metabolic network model with Elasticsearch
    def es_index_sbml(self, _, model):
        docid = model['name'] if 'name' in model else model['id']
        try:
            self.es.index(index=self.index, doc_type=self.doctype,
                          id=docid, body=json.dumps(model))
            return True
        except Exception as e:
            print(e)
        return False

    # Index metabolic network model with MongoDB
    def mongodb_index_sbml(self, _, model):
        docid = model['name'] if 'name' in model else model['id']
        spec = {"_id": docid}
        try:
            self.mcl.update(spec, model, upsert=True)
            return True
        except Exception as e:
            print(e)
        return False


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Index metabolic network files (SBML, or PSAMM yaml)'
                    ' with Elasticsearch or MongoDB')
    parser.add_argument('-infile', '--infile',
                        help='Input file or folder name'
                             ' with Metabolic network file(s) in PSAMM .yaml'
                             ' or SBML .xml formats')
    parser.add_argument('--index',
                        default=INDEX,
                        help='Name of the Elasticsearch index'
                             ' or MongoDB database')
    parser.add_argument('--doctype',
                        default=DOCTYPE,
                        help='Name for the Elasticsearch document type or '
                             'MongoDB collection')
    parser.add_argument('--host',
                        help='Elasticsearch or MongoDB server hostname')
    parser.add_argument('--port',
                        help="Elasticsearch or MongoDB server port number")
    parser.add_argument('--db', default='Elasticsearch',
                        help="Database: 'Elasticsearch' or 'MongoDB'")
    args = parser.parse_args()
    indxr = SBMLIndexer(args.db, args.index, args.doctype, args.host, args.port)
    indxr.read_and_index_model_files(args.infile)

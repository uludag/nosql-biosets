#!/usr/bin/env python
"""Index InterPro XML files, with Elasticsearch or MongoDB"""

import argparse
import traceback
from gzip import GzipFile

import xmltodict
from pymongo import IndexModel

from nosqlbiosets.dbutils import DBconnection
from nosqlbiosets.objutils import num, unifylistattribute

MDBCOLLECTION = 'interpro'


class Indexer(DBconnection):

    def __init__(self, db, esindex, mdbdb='biosets',
                 mdbcollection=MDBCOLLECTION,
                 host=None, port=None, recreateindex=True):
        self.mdbcollection = mdbcollection
        self.index = esindex if db == 'Elasticsearch' else mdbdb
        self.db = db
        indxcfg = {  # for Elasticsearch
            "index.number_of_replicas": 0
        }
        super(Indexer, self).__init__(db, self.index, host, port,
                                      es_indexsettings=indxcfg,
                                      recreateindex=recreateindex)
        if db == "MongoDB":
            self.mcl = self.mdbi[self.mdbcollection]
            self.mcl.drop()

    # Read and Index entries in InterPro xml file
    def parse_interpro_xmlfiles(self, infile):
        infile = str(infile)
        print("Reading/indexing %s " % infile)
        if infile.endswith(".gz"):
            with GzipFile(infile) as inf:
                xmltodict.parse(inf, item_depth=2,
                                item_callback=self.index_interpro_entry,
                                attr_prefix='')
        else:
            with open(infile, 'rb') as inf:
                xmltodict.parse(inf, item_depth=2,
                                item_callback=self.index_interpro_entry,
                                attr_prefix='')
        print("\nCompleted")

    def index_interpro_entry(self, _c, entry):
        def index(__c, _entry):
            try:
                assert len(__c) > 1 and len(__c[1]) > 1
                _entry.update(__c[1][1])
                docid = _entry['id']
                self.update_entry(_entry)
                if self.db == "Elasticsearch":
                    self.es.index(index=self.index,
                                  op_type='create', ignore=409,
                                  filter_path=['hits.hits._id'],
                                  id=docid, body=_entry)
                else:  # assume MongoDB
                    _entry["_id"] = docid
                    self.mcl.insert_one(_entry)
            except Exception as e:
                print("ERROR: %s" % e)
                print(traceback.format_exc())
                exit(-1)
            self.reportprogress(1000)
        if _c[1][0] == 'release' or _c[1][0] == 'deleted_entries':
            # Skip 'release'/'dbinfo' records at the beginning of the xml file
            #   "  'deleted_entries' records at the end of the xml file
            return True
        else:
            index(_c, entry)
        return True

    def update_entry(self, entry):
        del entry['id']
        num(entry, "protein_count", int)
        if 'abstract' in entry:
            import json
            entry['abstract'] = json.dumps(entry['abstract'])  # , indent=4)
        if 'taxonomy_distribution' in entry:
            unifylistattribute(entry, 'taxonomy_distribution', 'taxon_data')
            for i in entry['taxonomy_distribution']:
                num(i, "proteins_count", int)


def mongodb_indices(mdb):
    index = IndexModel([
        ("name", "text"),
        ("taxonomy_distribution.taxon_data.name", "text")
    ], name='text')
    mdb.create_indexes([index])
    indx_fields = [
        "name",
        "class_list.classification.id",
        "class_list.classification.class_type",
        "class_list.classification.category"
    ]
    for field in indx_fields:
        mdb.create_index(field)


def main(infile, dbtype, esindex, mdbcollection, mdbdb='biosets',
         host=None, port=None, recreateindex=True):
    indxr = Indexer(dbtype, esindex, mdbdb=mdbdb,
                    host=host, port=port, mdbcollection=mdbcollection,
                    recreateindex=recreateindex)
    indxr.parse_interpro_xmlfiles(infile)
    if dbtype == 'Elasticsearch':
        indxr.es.indices.refresh(index=esindex)
    else:
        mongodb_indices(indxr.mcl)


if __name__ == '__main__':
    from nosqlbiosets.dbutils import DBconnection, dbargs
    args = argparse.ArgumentParser(
        description='Index InterPro xml files,'
                    ' with Elasticsearch or MongoDB')
    args.add_argument('infile',
                      help='Input file name for interpro'
                           ' xml file')
    dbargs(args)
    args = args.parse_args()
    main(args.infile, args.dbtype, args.esindex, args.mdbcollection,
         args.mdbdb, args.host, args.port, args.recreateindex)

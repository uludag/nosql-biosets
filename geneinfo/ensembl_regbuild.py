#!/usr/bin/env python
""" Index Ensembl regulatory build GFF files with Elasticsearch"""
from __future__ import print_function

import argparse
from os import path

import gffutils
from elasticsearch.helpers import streaming_bulk

from nosqlbiosets.dbutils import DBconnection

chunksize = 2048


# Return db connection to the gffutils sqlite db for the given gff file
def connectgffdb(gff):
    gffdb = gff + ".db"
    if path.exists(gffdb):
        dbc = gffutils.FeatureDB(gffdb)
    else:
        dbc = gffutils.create_db(gff, gff + '.db',
                                 disable_infer_transcripts=True,
                                 disable_infer_genes=True,
                                 keep_order=True,
                                 verbose=False)
    return dbc


# Reader for transcription factors
def tfs_reader(db):
    for i in db.all_features():
        r = {
            '_id': i.id,
            "chr": i.seqid,
            "strand": i.strand,
            "start": i.start,
            "end": i.end,
            "tf": i.attributes["motif_feature_type"]
        }
        # TODO: merge readers? attributes differ only?
        yield r


# Reader for regulatory regions
def regregions_reader(db):
    for i in db.all_features():
        r = {
            '_id': i.id,
            "chr": i.seqid,
            "strand": i.strand,
            "start": i.start,
            "end": i.end,
            "feature_type": i.attributes["feature_type"]
        }
        yield r


def checkindex(es, index):
    if es.indices.exists(index=index):
        es.indices.delete(index=index, params={"timeout": "10s"})
    indxcfg = {"settings": {
        "index.number_of_replicas": 0, "index.refresh_interval": '360s'}}
    es.indices.create(index=index, params={"timeout": "10s"},
                      ignore=400, wait_for_active_shards=1)
    es.indices.put_settings(index=index, body=indxcfg)


def es_index(es, index, gffdb, reader, doctype):
    checkindex(es, index)
    for ok, result in streaming_bulk(
            es, reader(gffdb),
            index=index, doc_type=doctype, chunk_size=chunksize
    ):
        if not ok:
            action, result = result.popitem()
            doc_id = '/%s/commits/%s' % (args.index, result['_id'])
            print('Failed to %s document %s: %r' % (action, doc_id, result))
    es.indices.refresh(index=index)
    return


def main(db, infile, index, gfftype, host=None, port=None):
    if db in ["Elasticsearch"]:
        con = DBconnection("Elasticsearch", index,
                           host=host, port=port)
        gffdb = connectgffdb(infile)
        if gfftype == "transcriptionfactor":
            reader = tfs_reader
            doctype = "transcriptionfactor"
        elif gfftype == "regulatoryregion":
            reader = regregions_reader
            doctype = "regulatoryregion"
        else:
            print("gfftype should be 'transcriptionfactor'"
                  " or 'regulatoryregion'")
            return
        es_index(con.es, index, gffdb, reader, doctype)
        es_index(con.es, index, gffdb, reader, doctype)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Index Ensembl regulatory build '
                    'gff files using Elasticsearch')
    parser.add_argument('--infile',
                        help='Transcription factors binding sites or '
                             'Regulatory regions gff file')
    parser.add_argument('--index',
                        default="ensregbuild",
                        help='Name of the Elasticsearch index')
    parser.add_argument('--gfftype',
                        help='Type of the gff file, should be'
                             ' "transcriptionfactor" or "regulatoryregion"')
    parser.add_argument('--db', default='Elasticsearch',
                        help="Database: only 'Elasticsearch' is supported")
    parser.add_argument('--host',
                        help='Elasticsearch server hostname')
    parser.add_argument('--port',
                        help="Elasticsearch server port")
    args = parser.parse_args()
    main(args.db, args.infile, args.index, args.gfftype, args.host, args.port)

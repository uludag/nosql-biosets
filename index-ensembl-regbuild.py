#!/usr/bin/env python
""" Script to index Ensembl regulatory build TF binding sites """
from __future__ import print_function

import argparse
from os import path

import gffutils
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

# Document type name for the Elascticsearch index entries
doctype = 'ensembl-tfbs'
chunksize = 2 * 1024
gff = "./data/hg38.ensrb_motiffeatures.r87.gff"


def connectgffdb():
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


def iterate_over_tfbs(l):
    for i in db.all_features():
        print(i)
        yield {
            '_id': i.id,
            "chr": i.seqid,
            "strand": i.strand,
            "start": i.start,
            "end": i.end,
            "tf": i.attributes["motif_feature_type"]
        }


def es_index_tfbs(es, l):
    r = 0
    for ok, result in streaming_bulk(
            es,
            iterate_over_tfbs(l),
            index=args.index,
            doc_type=doctype,
            chunk_size=chunksize
    ):
        action, result = result.popitem()
        doc_id = '/%s/commits/%s' % (args.index, result['_id'])
        # process the information returned from ES whether the document has been
        # successfully indexed
        if not ok:
            print('Failed to %s document %s: %r' % (action, doc_id, result))
    return 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Index Ensembl TF binding sites using Elasticsearch')
    parser.add_argument('--infile',
                        default=gff,
                        help='input file to index')
    parser.add_argument('--index',
                        default="ensembl-tfbs",
                        help='name of the Elasticsearch index')
    parser.add_argument('--host', default="esnode-khadija",
                        help='Elasticsearch server hostname')
    parser.add_argument('--port', default="9200",
                        help="Elasticsearch server port")
    args = parser.parse_args()
    host = args.host
    port = args.port
    con = Elasticsearch(host=host, port=port, timeout=3600)
    db = connectgffdb()
    es_index_tfbs(con, db)

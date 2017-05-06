#!/usr/bin/env python
""" Index Ensembl regulatory build GFF files """
from __future__ import print_function

import argparse
import json
from os import path

import gffutils
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

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
def tfs(db):
    for i in db.all_features():
        r = {
            '_id': i.id,
            "chr": i.seqid,
            "strand": i.strand,
            "start": i.start,
            "end": i.end,
            "tf": i.attributes["motif_feature_type"]
        }
        yield r


# Reader for regulatory regions
def regregions(db):
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


def es_index(es, l, reader, doctype):
    for ok, result in streaming_bulk(
            es,
            reader(l),
            index=args.index,
            doc_type=doctype,
            chunk_size=chunksize
    ):
        action, result = result.popitem()
        doc_id = '/%s/commits/%s' % (args.index, result['_id'])
        if not ok:
            print('Failed to %s document %s: %r' % (action, doc_id, result))
    return 1


if __name__ == '__main__':
    conf = {"host": "localhost", "port": 9200}
    try:
        conf = json.load(open("conf/elasticsearch.json", "rt"))
    finally:
        pass

    parser = argparse.ArgumentParser(
        description='Index Ensembl Transcription Factors binding sites '
                    'gff file using Elasticsearch')
    parser.add_argument('--motifsgff',
                        default="./data/hg38.ensrb_motiffeatures.r87.gff",
                        help='Transcription Factors binding sites gff file')
    parser.add_argument('--regregionssgff',
                        default="./data/hg38.ensrb_features.r87.gff",
                        help='Regulatory regions gff file to index')
    parser.add_argument('--index',
                        default="ensregbuild",
                        help='name of the Elasticsearch index')
    parser.add_argument('--host', default=conf['host'],
                        help='Elasticsearch server hostname')
    parser.add_argument('--port', default=conf['port'],
                        help="Elasticsearch server port")
    args = parser.parse_args()
    host = args.host
    port = args.port
    con = Elasticsearch(host=host, port=port, timeout=3600)
    tfbsdb = connectgffdb(args.motifsgff)
    es_index(con, tfbsdb, tfs, "transcriptionfactor")
    regregionsdb = connectgffdb(args.regregionsgff)
    es_index(con, regregionsdb, regregions, "regulatoryregion")

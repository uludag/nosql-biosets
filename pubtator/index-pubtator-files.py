#!/usr/bin/env python
# Index NCBI PubTator gene2pub associations file with Elasticsearch

import argparse
import gzip
import json
import os

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

# Document type name for the Elascticsearch index entries
doctype = 'gene2pub'
ChunkSize = 2*1024


# Read given PubTator file, index using the index function specified
def read_and_index_pubtator_file(infile, es, indexfunc):
    if infile.endswith(".gz"):
        f = gzip.open(infile, 'rt')
    else:
        f = open(infile, 'r')
    r = indexfunc(es, f)
    return r


def parse_pub2gene_lines(f, r):
    for line in f:
        a = line.strip('\n').split('\t')
        if len(a) != 4:
            print("Line has more than 4 fields: %s" % line)
            exit(-1)
        if r > 0: # skip header line
            geneids = a[1].replace(';', ',').split(',')
            yield {
                '_id': r,
                "pmid": a[0], "geneid": geneids,
                "mentions": a[2].split('|'),
                "resource": a[3].split('|')
            }
        r += 1


def es_index(es, f):
    r = 0
    for ok, result in streaming_bulk(
            es,
            parse_pub2gene_lines(f, r),
            index=args.index,
            doc_type='gene2pub',
            chunk_size=ChunkSize
    ):
        action, result = result.popitem()
        doc_id = '/%s/commits/%s' % (args.index, result['_id'])
        # process the information from ES whether the document has been
        # successfully indexed
        if not ok:
            print('Failed to %s document %s: %r' % (action, doc_id, result))
        else:
            r += 1
    return r


def main(es, infile, index):
    #es.indices.delete(index=index, params={"timeout": "10s"})
    i = es.info()
    v = int(i['version']['number'][0])
    if v >= 5:
        iconfig = json.load(open(d + "/../mappings/pubtator.json", "rt"))
    else:
        iconfig = json.load(open(d + "/../mappings/pubtator-es2.json", "rt"))
    es.indices.create(index=index, params={"timeout": "20s"},
                      ignore=400, body=iconfig, wait_for_active_shards=1)
    read_and_index_pubtator_file(infile, es, es_index)
    es.indices.refresh(index=index)


if __name__ == '__main__':
    conf = {"host": "localhost", "port": 9200}
    try:
        d = os.path.dirname(os.path.abspath(__file__))
        print(d)
        conf = json.load(open(d + "/../conf/elasticsearch.json", "rt"))
    finally:
        pass
    parser = argparse.ArgumentParser(
        description='Index NCBI PubTator files using Elasticsearch')
    parser.add_argument('--infile',
                        default=d + "/../data/gene2pubtator.sample",
                        help='input file to index')
    parser.add_argument('--index',
                        default="pubtator-gene2pub",
                        help='name of the Elasticsearch index')
    parser.add_argument('--host', default=conf['host'],
                        help='Elasticsearch server hostname')
    parser.add_argument('--port', default=conf['port'],
                        help="Elasticsearch server port")
    args = parser.parse_args()
    host = args.host
    port = args.port
    con = Elasticsearch(host=host, port=port, timeout=3600)
    main(con, args.infile, args.index)

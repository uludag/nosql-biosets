#!/usr/bin/python3

import argparse
import gzip
import json
import os
import time
from zipfile import ZipFile
from elasticsearch import Elasticsearch


# Read PubChem Bioassays json files, index using the index function specified
# Iterate over files in the folder if the input file is a folder
def read_and_index_pubchem_bioassays(infile, es, indexfunc):
    print("Reading %s " % infile)
    i = 0
    t1 = time.time()
    if os.path.isdir(infile):
        for child in os.listdir(infile):
            if child.endswith(".zip"):
                c = os.path.join(infile, child)
                read_and_index_pubchem_bioassays_zipfile(c, es, indexfunc)
            else:
                read_and_index_pubchem_bioassays_file(child, es, indexfunc)
            i += 1
    else:
        if infile.endswith(".zip"):
            read_and_index_pubchem_bioassays_zipfile(infile, es, indexfunc)
        else:
            read_and_index_pubchem_bioassays_file(infile, es, indexfunc)
        i = 1
    t2 = time.time()
    print("-- %d files have been processed, in %dms"
          % (i, (t2 - t1) * 1000))
    return None


# Read PubChem Bioassays zip file, index using the index function specified
def read_and_index_pubchem_bioassays_zipfile(zipfile, es, indexfunc):
    print("\nProcessing %s " % zipfile)
    i = 0
    r = 0
    with ZipFile(zipfile) as myzip:
        for fname in myzip.namelist():
            aid = fname[fname.find('/')+1:fname.find(".json")]
            # check whether the entry is already indexed
            if not es.exists(index=args.index, doc_type='bioassay', id=aid):
                with myzip.open(fname) as jfile:
                    f = gzip.open(jfile, 'rt')
                    ba = json.load(f)
                    r = indexfunc(es, ba, r+f.tell(), aid)
                    i += 1
            else: print("-", end='', flush=True)
    return i


# Read PubChem Bioassays file, index using the index function specified
def read_and_index_pubchem_bioassays_file(infile, es, indexfunc):
    print("Reading %s " % infile)
    if infile.endswith(".gz"):
        f = gzip.open(infile, 'rt')
    else:
        f = open(infile, 'r')
    ba = json.load(f)
    r = indexfunc(es, ba)
    return r


def es_index_bioassay(es, ba, r, aid):
    if aid != ba['PC_AssaySubmit']['assay']['descr']['aid']['id']:
        print("filename and Assay ids are different, please check '%s'" % aid)
        exit(-1)
    try:
        if r > 6666*6:  # since some entries are huge refresh often
            print("r", end='', flush=True)
            es.indices.refresh(index=args.index)
            es.indices.clear_cache(index=args.index)
            r = 0
        print(".", end='', flush=True)
        es.index(index=args.index, doc_type='bioassay',
                 id=aid, body=json.dumps(ba))
    except Exception as e:
        print(e)
    return r


def main(es, infile, index):
    # es.indices.delete(index=index, params={"timeout": "10s"})
    iconfig = json.load(open("pubchem-bioassays-index-config.json", "rt"))
    es.indices.create(index=index, params={"timeout": "20s"},
                      ignore=400, body=iconfig)
    read_and_index_pubchem_bioassays(infile, es, es_index_bioassay)
    es.indices.refresh(index=index)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Index PubChem Bioassays, using Elasticsearch')
    parser.add_argument('--infile',
                        # default="1158083.json",
                        # default="pubchem/1158001_1159000/",
                        default="/reference/NCBI/pubchem/Bioassay/JSON/",
                        #default="/reference/NCBI/pubchem/Bioassay/JSON/0000001_0001000.zip",
                        help='input file to index')
    parser.add_argument('--index',
                        default="pubchem-bioassays-test12",
                        help='name of the elasticsearch index')
    parser.add_argument('--host', default="esnode-khadija",
                        help='Elasticsearch server hostname')
    parser.add_argument('--port', default="9200",
                        help="Elasticsearch server port")
    args = parser.parse_args()
    host = args.host
    port = args.port
    con = Elasticsearch(host=host, port=port, timeout=3600)
    main(con, args.infile, args.index)

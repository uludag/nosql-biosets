#!/usr/bin/env python
"""Index WikiPathways gpml files"""
import argparse
import gzip
import json
import os
import re
import time
from xml.etree.ElementTree import fromstring
from zipfile import ZipFile

from elasticsearch import Elasticsearch
from xmljson import yahoo


# Read WikiPathways xml file, index using the function indexf
# If the input file is a folder iterate over files in the folder
def read_and_index_pathways(infile, es, indexf, index):
    print("Reading %s " % infile)
    i = 0
    t1 = time.time()
    if os.path.isdir(infile):
        for child in os.listdir(infile):
            c = os.path.join(infile, child)
            if child.endswith(".zip"):
                i += read_and_index_wikipathways_zipfile(c, es, indexf, index)
            else:
                read_and_index_wikipathways_file(c, es, indexf, index)
                i += 1
    elif infile.endswith(".zip"):
        i += read_and_index_wikipathways_zipfile(infile, es, indexf, index)
    else:
        read_and_index_wikipathways_file(infile, es, indexf, index)
        i = 1
    t2 = time.time()
    print("-- %d files have been processed, in %dms"
          % (i, (t2 - t1) * 1000))
    return None


# Read WikiPathways file, index using the function indexf
def read_and_index_wikipathways_file(infile, es, indexf, index):
    infile = str(infile)
    print("Reading %s " % infile)
    if infile.endswith(".gz"):
        f = gzip.open(infile, 'rt')
    else:
        f = open(infile, 'r')
    xml_ = f.read()
    xml = re.sub(' xmlns="[^"]+"', '', xml_, count=1)
    # ba = xmltodict.parse(xml)
    # ba = untangle.parse(xml)
    ba = yahoo.data(fromstring(xml))
    r = indexf(es, ba, infile, index)
    # todo: new function to avoid code duplicate
    return r


def es_index_pathway(es, ba, docid, index):
    # ignore Biopax field
    del(ba["Pathway"]["Biopax"])
    try:
        es.index(index=index, doc_type='wikipathways',
                 id=docid, body=json.dumps(ba))
        r = 1
    except Exception as e:
        print(e)
        r = 0
    return r


# TODO: remove 'Graphics' and 'GraphId' elements
# Read WikiPathways zipfile, index using the function indexf
def read_and_index_wikipathways_zipfile(zipfile, es, indexf, index):
    i = 0
    with ZipFile(zipfile) as myzip:
        for fname in myzip.namelist():
            print("Reading %s " % fname)
            with myzip.open(fname) as jfile:
                xml_ = jfile.read()
                if not isinstance(xml_, str):
                    xml_ = xml_.decode('utf-8')
                xml = re.sub(' xmlns="[^"]+"', '', xml_, count=1)
                # ba = xmltodict.parse(xml)
                # ba = untangle.parse(xml.decode('utf-8'))
                ba = yahoo.data(fromstring(xml))
                r = indexf(es, ba, jfile.name, index)
                i += r
    return i


def main(es, infile, index):
    # es.indices.delete(index=index, params={"timeout": "10s"})
    es.indices.create(index=index, params={"timeout": "10s"},
                      ignore=400, body={"settings": {"number_of_replicas": 0}})
    read_and_index_pathways(infile, es, es_index_pathway, index)
    es.indices.refresh(index=index)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Index WikiPathways entries, using Elasticsearch')
    parser.add_argument('-infile', '--infile',
                        default="./wikipathways/data/",
                        help='Input file or folder name'
                             ' with WikiPathways file(s)'
                             ' (zip files are also supported)')
    parser.add_argument('--index',
                        default="wikipathways",
                        help='Name of the Elasticsearch index')
    parser.add_argument('--host', default="localhost",
                        help='Elasticsearch server hostname')
    parser.add_argument('--port', default="9200",
                        help="Elasticsearch server port number")
    args = parser.parse_args()
    host = args.host
    port = args.port
    con = Elasticsearch(host=host, port=port, timeout=120)
    main(con, args.infile, args.index)

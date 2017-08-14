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
from nosqlbiosets.dbutils import DBconnection
from xmljson import yahoo


# Read WikiPathways xml file, index using the function indexf
# If the input file is a folder iterate over files in the folder
def read_and_index_pathways(infile, dbc, indexf, index):
    i = 0
    t1 = time.time()
    if os.path.isdir(infile):
        for child in os.listdir(infile):
            c = os.path.join(infile, child)
            if child.endswith(".zip"):
                i += read_and_index_wikipathways_zipfile(c, dbc, indexf, index)
            else:
                read_and_index_wikipathways_file(c, dbc, indexf, index)
                i += 1
    elif infile.endswith(".zip"):
        i += read_and_index_wikipathways_zipfile(infile, dbc, indexf, index)
    else:
        read_and_index_wikipathways_file(infile, dbc, indexf, index)
        i = 1
    t2 = time.time()
    print("-- %d files have been processed, in %dms"
          % (i, (t2 - t1) * 1000))
    return None


# TODO: remove 'Graphics' and 'GraphId' elements
# Read WikiPathways zipfile, index using the function indexf
def read_and_index_wikipathways_zipfile(zipfile, dbc, indexf, index):
    i = 0
    with ZipFile(zipfile) as myzip:
        for fname in myzip.namelist():
            print("Reading %s " % fname)
            with myzip.open(fname) as jfile:
                xml = jfile.read()
                if not isinstance(xml, str):
                    xml = xml.decode('utf-8')
                r = read_and_index_wikipathways_xml(xml, dbc, indexf, index)
                i += r
    return i


# Read WikiPathways file, index using the function indexf
def read_and_index_wikipathways_file(infile, dbc, indexf, index):
    infile = str(infile)
    print("Reading %s " % infile)
    if infile.endswith(".gz"):
        f = gzip.open(infile, 'rt')
    else:
        f = open(infile, 'r')
    xml = f.read()
    r = read_and_index_wikipathways_xml(xml, dbc, indexf, index)
    return r


# Index WikiPathways xml using the function indexf
def read_and_index_wikipathways_xml(xml, es, indexf, index):
    xml = re.sub(' xmlns="[^"]+"', '', xml, count=1)
    pathway = yahoo.data(fromstring(xml))["Pathway"]
    # Delete fields that would normally be used for rendering images
    for a in ["Biopax", "BiopaxRef", "Graphics", "Shape", "Group", "InfoBox"]:
        if a in pathway:
            del pathway[a]
    for a in ["Interaction", "DataNode", "Label"]:
        if a in pathway:
            for i in pathway[a]:
                if isinstance(i, str):
                    continue
                del i["Graphics"]
                if "GraphId" in i:
                    del i["GraphId"]
    r = indexf(es, pathway, pathway["Name"], index)
    return r


doctype = 'wikipathway'


def es_index_pathway(dbc, pathways, docid, index):
    try:
        dbc.es.index(index=index, doc_type=doctype,
                     id=docid, body=json.dumps(pathways))
        r = 1
        dbc.es.indices.refresh(index=index)
    except Exception as e:
        print(e)
        r = 0
    return r


def mongodb_index_pathway(dbc, ba, docid, index):
    spec = {"_id": docid}
    # try:
    dbc.mdbi[doctype].update(spec, ba, upsert=True)
    r = 1
    # except Exception as e:
    #     print(e)
    #     r = 0
    return r


def main(db, infile, index, host, port):
    dbc = DBconnection(db, index, host, port, recreateindex=True)
    if db == "Elasticsearch":
        read_and_index_pathways(infile, dbc, es_index_pathway, index)
    else:
        read_and_index_pathways(infile, dbc, mongodb_index_pathway, index)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Index WikiPathways entries, using Elasticsearch')
    parser.add_argument('-infile', '--infile',
                        help='Input file or folder name'
                             ' with WikiPathways file(s)'
                             ' (zip files are also supported)')
    parser.add_argument('--index',
                        default="wikipathways",
                        help='Name of the Elasticsearch index')
    parser.add_argument('--host',
                        help='Elasticsearch server hostname')
    parser.add_argument('--port',
                        help="Elasticsearch server port number")
    parser.add_argument('--db', default='Elasticsearch',
                        help="Database: 'Elasticsearch' or 'MongoDB'")
    args = parser.parse_args()
    main(args.db, args.infile, args.index, args.host, args.port)

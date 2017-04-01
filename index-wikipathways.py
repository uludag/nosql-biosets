#!/usr/bin/python3
import gzip, json
import os, re, time, argparse
from zipfile import ZipFile
from elasticsearch import Elasticsearch
#import xmltodict
# import untangle:
# Element(name = None, attributes = None, cdata = ) is not JSON serializable
from xmljson import yahoo           # == xmljson.Yahoo()
#from xmljson import gdata           # == xmljson.GData()
#from xmljson import badgerfish      # == xmljson.BadgerFish()
#from xmljson import parker          # == xmljson.Parker()

from xml.etree.ElementTree import fromstring


# Read WikiPathways xml file, index using the function indexf
# If the input file is a folder iterate over files in the folder
def read_and_index_pathways(infile, es, indexf):
    print("Reading %s " % infile)
    i = 0
    t1 = time.time()
    if os.path.isdir(infile):
        for child in os.listdir(infile):
            c = os.path.join(infile, child)
            if child.endswith(".zip"):
                i += read_and_index_wikipathways_zipfile(c, es, indexf)
            else:
                read_and_index_wikipathways_file(c, es, indexf)
                i += 1
    else:
        read_and_index_wikipathways_file(infile, es, indexf)
        i = 1
    t2 = time.time()
    print("-- %d files have been processed, in %dms"
          % (i, (t2 - t1) * 1000))
    return None


# Read Wikipathways file, index using the function indexf
def read_and_index_wikipathways_file(infile_, es, indexf):
    infile = str(infile_)
    print("Reading %s " % infile)
    if infile.endswith(".gz"):
        f = gzip.open(infile, 'rt')
    else:
        f = open(infile, 'r')
    xml_ = f.read()
    xml = re.sub(' xmlns="[^"]+"', '', xml_, count=1)
    # ba = xmltodict.parse(xml)
    #ba = untangle.parse(xml)
    ba = yahoo.data(fromstring(xml))
    r = indexf(es, ba, infile_)
    # todo: new function to avoid code duplicate
    return r


def es_index_pathway(es, ba, docid):
    id = docid
    #print(json.dumps(ba["Pathway"]["DataNode"], indent=2))
    del(ba["Pathway"]["Biopax"])
    try:
        es.index(index=index, doc_type='wikipathways',
                 id=id, body=json.dumps(ba))
        r = 1
    except Exception as e:
        print(e)
        r = 0
    return r


def main(es, infile, index):
    # es.indices.delete(index=index, params={"timeout": "10s"})
    iconfig = json.load(open("wikipathways-index-config.json", "rt"))
    es.indices.create(index=index, params={"timeout": "10s"},
                      ignore=400, body=iconfig)
    read_and_index_pathways(infile, es, es_index_pathway)
    es.indices.refresh(index=index)


# Read Wikipathways zipfile, index using the index-function
def read_and_index_wikipathways_zipfile(zipfile, es, indexf):
    print("Reading %s " % zipfile)
    i = 0
    with ZipFile(zipfile) as myzip:
        for fname in myzip.namelist():
            with myzip.open(fname) as jfile:
                f = jfile  #  open(jfile, 'rt')
                xml_ = f.read().decode('utf-8')
                xml = re.sub(' xmlns="[^"]+"', '', xml_, count=1)
                # ba = xmltodict.parse(xml)
                #ba = untangle.parse(xml.decode('utf-8'))
                ba = yahoo.data(fromstring(xml))
                r = indexf(es, ba, jfile.name)
                # print(r)
                i += r
    return i


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Index WikiPathways entries, using Elasticsearch')
    parser.add_argument('-infile', '--infile',
                        # default="Hs_SUMOylation_of_chromatin_organization_proteins_WP3799_86403.gpml",
                        default="./wikipathways/data/",
                        #default="./wikipathways_Arabidopsis_thaliana_Curation-AnalysisCollection__gpml.zip",
                        help='input file name or folder with WikiPathways files')
    parser.add_argument('--index',
                        default="wikipathways",
                        help='name of the Elasticsearch index')
    parser.add_argument('--host', default="localhost",
                        help='Elasticsearch server hostname')
    parser.add_argument('--port', default="9200",
                        help="Elasticsearch server port")
    args = parser.parse_args()
    infile = args.infile
    index = args.index
    host = args.host
    port = args.port
    con = Elasticsearch(host=host, port=port, timeout=120)
    main(con, infile, index)

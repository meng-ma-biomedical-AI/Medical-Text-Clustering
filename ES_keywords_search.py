from tqdm import tqdm
import os
import re
import time
import codecs
import jieba
import pickle
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
import pymysql
import mysql.connector as sql

time_1 = time.time()

es = Elasticsearch([{'host':'localhost','port':9200}])

es_idx = IndicesClient(es)

es_idx.delete(index='_all')

a = os.popen('''
curl -X PUT 'localhost:9200/pubmed_articles' -d '
{
  "mappings": {
    "docs": {
      "properties": {
        "pmid": {
          "type": "text"
        },
        "title": {
          "type": "text",
          "analyzer": "ik_smart",
          "search_analyzer": "ik_smart"
        }
      }
    }
  }
}'
''').read()
for line in a.split('\n'):
    print(line)

# fetch all data from mysql:
conn = pymysql.connect(host = 'rm-2zet5lw17as40fty28o.mysql.rds.aliyuncs.com',
                       port = 3306,
                       user = 'snowball',
                       passwd = 'MEDOsnow$%^&',
                       db = 'medo_master')
cur = conn.cursor()
sql_command = "SELECT pmid, title FROM pubmed_articles"

cur.execute(sql_command)
res = cur.fetchall()
print(len(res))

doc_list = []
for item in res:
    doc = {"pmid": item[0], "title": item[1]}
    doc_list.append(doc)
print(len(doc_list))
print(doc_list[0])

for doc in doc_list:
    temp = os.popen('''
    curl -X POST 'localhost:9200/pubmed_articles/docs' -d '
    {}
    '
    '''.format(str(doc).replace("\"", "\'").replace("\'", "\""))).read()
    for line in temp.split('\n'):
        print(line)


print('all time: ', time.time() - time_1)

# tokens_res = es_idx.analyze(index='pubmed_articles', body={
# #     "analyzer": "ik_max_word",
#     "analyzer": "ik_smart",
#     "text" : "HiFreSP: A novel high-frequency sub-pathway mining approach to identify robust prognostic gene signatures."
# })
# for token in tokens_res['tokens']:
#     print(token['token'])
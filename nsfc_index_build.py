import os
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
import mysql.connector as sql

es = Elasticsearch([{'host':'localhost','port':9200}])
es_idx = IndicesClient(es)
es_idx.delete(index='_all')
a = os.popen('''
curl -X PUT 'localhost:9200/main_idx' -d '
{
  "mappings": {
    "docs": {
      "properties": {
        "pure_text": {
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
    
db_connection = sql.connect(host='rm-2zet5lw17as40fty28o.mysql.rds.aliyuncs.com',
                            database='medo_master', 
                            user='snowball', 
                            password='MEDOsnow$%^&')
nsfc_data = pd.read_sql('SELECT summaryCh FROM nsfc_data', con = db_connection)
nsfc_data.dropna(axis=0, inplace=True)

doc_list = []
for idx,row in nsfc_data.iterrows():
    doc = {"pure_text": row['summaryCh']}
    doc_list.append(doc)

for doc in doc_list:
    temp = os.popen('''
    curl -X POST 'localhost:9200/main_idx/docs' -d '
    {}
    '
    '''.format(str(doc).replace("\"", "\'").replace("\'", "\""))).read()
    for line in temp.split('\n'):
        print(line)
from tqdm import tqdm
import os
import re
import time
import json
import codecs
import jieba
import pickle
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
import pymysql
import mysql.connector as sql

def fetch_mysql_data(db_conn, table_name, table_fields):
    '''
    :param db_conn:
    :param table_name:
    :param table_fields:
    :return:
    '''
    cur = db_conn.cursor()
    sql_command = " SELECT " + ",".join(table_fields) + " FROM " + table_name

    cur.execute(sql_command)
    fetch_results = list(cur.fetchall())

    return fetch_results

def json_data_gen(all_data, data_table_name, data_fields, data_start_id):
    '''
    :param all_data:
    :param data_table_name:
    :param data_fields:
    :return:
    '''
    with open('./json_data_temp.json', 'w', encoding='utf-8') as f:
        for idx, item in enumerate(all_data):
            index_line = json.dumps({"index": {"_index": data_table_name, "_type": "docs", "_id": str(data_start_id + idx + 1)}}) + '\n'
            data_dict = {}
            for i, field in enumerate(data_fields):
                data_dict[field] = item[i]
            data_line = json.dumps(data_dict) + '\n'
            f.writelines([index_line, data_line])

def build_index(post_batch, table_name, table_fields):
    '''
    :param post_batch:
    :param table_name:
    :param table_fields:
    :return:
    '''
    start_time = time.time()
    properties_dict = {}
    for field in table_fields:
        properties_dict[field] = {"type": "text",
                                  "analyzer": "ik_max_word",
                                  "search_analyzer": "ik_max_word"}
    index_body_dict = { "mappings": { "docs": { "properties": properties_dict } } }
    create_index_command = "curl -X PUT 'localhost:9200/" + table_name + "' -d '" + json.dumps(index_body_dict) + "'"
    os.system(create_index_command)

    conn = pymysql.connect(host='rm-2zet5lw17as40fty28o.mysql.rds.aliyuncs.com',
                           port=3306,
                           user='snowball',
                           passwd='MEDOsnow$%^&',
                           db='medo_master')

    res = fetch_mysql_data(conn, table_name, table_fields)

    for idx in range(0, len(res), post_batch):
        json_data_gen(res[idx : idx + post_batch], table_name, table_fields, idx)
        batch_post_command = "curl -XPOST localhost:9200/_bulk --data-binary @json_data_temp.json"
        os.system(batch_post_command)

    return (table_name, table_fields, len(res), time.time() - start_time)


def query_keyword(index_name, query_field, query_keyword):
    '''
    :param index_name:
    :param query_field:
    :param query_keyword:
    :return:
    '''
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    search_body_dict = { "query": {"term": {query_field : query_keyword} } }
    results = es.search(index = index_name, body = json.dumps(search_body_dict) )
    hits = results['hits']['hits']
    for idx, res in enumerate(hits):
        print(str(idx + 1) + ":  " + res['_source']['comSummary'])


if __name__ == '__main__':

    # table_name, table_fields, data_num, spend_time = build_index(20000, "nsfc_data", ["applyCode", "comSummary"])
    # print("Build Index Done.")
    # print("Index Table: ", table_name)
    # print("Index Table Fields: ", table_fields)
    # print("Number of Data: ", data_num)
    # print("Building Time: ", spend_time)

    query_keyword("nsfc_data", "comSummary", "心脏毒性")








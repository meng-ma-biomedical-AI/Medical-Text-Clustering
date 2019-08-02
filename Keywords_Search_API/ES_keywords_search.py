import os
import gc
import time
import json
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
import pymysql

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
    data_num = len(res)
    print("\nFetch Data Done.\n")

    for idx in range(0, data_num, post_batch):
        json_data_gen(res[idx : idx + post_batch], table_name, table_fields, idx)
        batch_post_command = "curl -XPOST localhost:9200/_bulk --data-binary @json_data_temp.json"
        os.system(batch_post_command)

    del res
    gc.collect()    # 索引建好后，释放从数据库中读取的数据所占用的内存

    return (table_name, table_fields, data_num, time.time() - start_time)


def query_keyword(index_name, query_field, query_keyword):
    '''
    :param index_name:
    :param query_field:
    :param query_keyword:
    :return:
    '''
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    search_body_dict = { "query": {"term": {query_field : query_keyword} } }
    # search_command = "curl 'localhost:9200/" + index_name + "/_search'  -d '" + json.dumps(search_body_dict) + "'"
    # results = json.loads(os.popen(search_command).read())
    results = es.search(index = index_name, body = json.dumps(search_body_dict) )
    # print(results)
    hits = results['hits']['hits']
    print(len(hits))
    for idx, res in enumerate(hits):
        print(str(idx + 1) + ":  " + res['_source'][query_field])


if __name__ == '__main__':

    table_name, table_fields, data_num, spend_time = build_index(400000, "wf_article", ["id", "title"])
    print("Build Index Done.")
    print("Index Table: ", table_name)
    print("Index Table Fields: ", table_fields)
    print("Number of Data: ", data_num)
    print("Building Time: ", spend_time)

    # query_keyword("wf_article", "title", "心脏毒性")








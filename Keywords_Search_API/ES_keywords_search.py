import os
import gc
import time
import json
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch.client import CatClient
import pymysql

def fetch_mysql_data(db_conn, table_name, table_fields):
    '''
    该函数用sql查询语句，取出table_name的所有数据，只取出每个数据对应的table_fields字段
    :param db_conn: pymysql数据库连接
    :param table_name: 待查询的数据库表名 type = string
    :param table_fields: 待查询的数据表的字段 type = list of string
    :return: 数据列表，每个数据包含table_fields中的字段
    '''
    cur = db_conn.cursor()
    sql_command = " SELECT " + ",".join(table_fields) + " FROM " + table_name

    cur.execute(sql_command)
    fetch_results = list(cur.fetchall())

    return fetch_results

def json_data_gen(all_data, data_table_name, data_fields, data_start_id):
    '''
    该函数用于生成临时json文件，该json文件是用来批量导入到es的；
    由于es的批量导入一次性支持的json文件大小有所限制（100M），所以当json数据文件大小超过100M，批量导入会失败，但不会报错。
    :param all_data: 待导入的数据（list of tuple），每个tuple为一条数据，包含了data_fields字段
    :param data_table_name: 待导入的数据表名，我们将该表名直接作为索引名（index name）
    :param data_fields: 待导入的数据表字段
    :param data_start_id: 由于是分批导入，所以输入每批数据的起始id，该批数据的索引id在此基础上递增，使得每条数据的索引id不同
    :return: 无返回值，直接在当前目录下生成临时json文件，该文件不断覆盖先前批次的json数据
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
    该函数用于批量构建索引，构建成功的前提是当前文件夹下存在"json_data_temp.json"数据文件，而且该文件大小不超过100M
    :param post_batch: 一批数据
    :param table_name: 当前数据的表名（同索引名）
    :param table_fields: 索引数据的字段
    :return: (索引名，索引字段，索引数据量，构建该数据表所用时间)
    '''
    start_time = time.time()
    properties_dict = {}
    for field in table_fields:
        properties_dict[field] = {"type": "text",
                                  "analyzer": "ik_max_word",
                                  "search_analyzer": "ik_max_word"}
    index_body_dict = { "mappings": { "docs": { "properties": properties_dict } } }

    create_index_command = "curl -X PUT 'localhost:9200/" + table_name + "' -d '" + \
                           json.dumps(index_body_dict) + "'"
    os.system(create_index_command)

    set_max_res_window = "curl -X PUT 'http://localhost:9200/" + table_name + "/_settings' -d '" + \
                         json.dumps({ "index" : { "max_result_window" : 1000000000 } }) + "'"
    os.system(set_max_res_window)

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

    # index_count_command = "curl -X GET 'http://localhost:9200/_cat/count/" + index_name + "?v'"
    # count = os.popen(index_count_command).read().split()[-1]
    # print(count)

    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    search_body_dict = { "query": {"term": {query_field : query_keyword} } }
    # search_command = "curl 'localhost:9200/" + index_name + "/_search'  -d '" + json.dumps(search_body_dict) + "'"
    # results = json.loads(os.popen(search_command).read())
    results = es.search(index = index_name, body = json.dumps(search_body_dict), size = 1000000000)
    # print(results)
    hits = results['hits']['hits']

    print(len(hits))
    # for idx, res in enumerate(hits):
    #     print(str(idx + 1) + ":  " + res['_source'][query_field])
    return hits
    # return results

if __name__ == '__main__':

    pass

    # conn = pymysql.connect(host='rm-2zet5lw17as40fty28o.mysql.rds.aliyuncs.com',
    #                        port=3306,
    #                        user='snowball',
    #                        passwd='MEDOsnow$%^&',
    #                        db='medo_master')

    # a = os.popen("curl -X GET 'http://localhost:9200/_cat/count/wf_article?v'").read()
    # print(a.split())
    # print(type(a))

    # table_name, table_fields, data_num, spend_time = build_index(400000, "wf_article", ["id", "title"])
    # print("Build Index Done.")
    # print("Index Table: ", table_name)
    # print("Index Table Fields: ", table_fields)
    # print("Number of Data: ", data_num)
    # print("Building Time: ", spend_time)

    # query_keyword("wf_article", "title", "心脏毒性")







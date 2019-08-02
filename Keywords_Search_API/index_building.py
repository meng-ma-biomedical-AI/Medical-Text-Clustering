from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from .ES_keywords_search import *

if __name__ == '__main__':

    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])   # 连接本地 ElasticSearch 端口
    es_idx = IndicesClient(es)
    es_idx.delete(index='_all') # 删除当前 ES 中的所有索引记录

    # wf_article表
    table_name, _, data_num, _ = build_index(300000, "wf_article", ["id", "title"])
    print(table_name + " Index Building Done.")
    print("Number of Data: ", data_num)

    # wf_article表
    table_name, _, data_num, _ = build_index(300000, "wf_article", ["id", "title"])
    print(table_name + " Index Building Done.")
    print("Number of Data: ", data_num)


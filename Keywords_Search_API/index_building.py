from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from ES_keywords_search import *

def build(post_batch, table_name, table_fields):
    '''
    :param post_batch: 批量构建索引时，每一批的数据量大小，int类型，需要人工设定；
    :param table_name: 待构建的数据库表名，string类型
    :param table_fields: 该数据库表中，需要检索的字段名，字符串列表类型，list of string;
                         注意：该参数的第一个字段必须为该数据库表的主键，唯一标识该表中的数据，如下面的"id","pmid","ct_id"等等，用来标识检索返回的json数据中的数据
    '''
    table_name, _, data_num, spend_time = build_index(post_batch, table_name, table_fields)
    print(table_name + " Index Building Done.")
    print("Number of Data: ", data_num)
    print("Spend Time: ", spend_time)


if __name__ == '__main__':

    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])   # 连接本地 ElasticSearch 端口
    es_idx = IndicesClient(es)

    # 下面调用 build 函数就是为不同表，以及表中的字段构建索引;
    # build 函数参数及用法介绍如上;

    # 文章：wf_article 和 pubmed_articles
    try:
        es_idx.delete(index="wf_article")  # 构建索引前，删除当前 ES 中的索引记录
    except:
        pass
    build(200000, "wf_article", ["id", "title", "keyword"])

    try:
        es_idx.delete(index="pubmed_articles")
    except:
        pass
    build(100000, "pubmed_articles", ["pmid", "title", "keywords", "mesh"])

    # CT： chinadrugtrial, chictr_main, ctgov
    try:
        es_idx.delete(index="chinadrugtrial")
    except:
        pass
    build(1000, "chinadrugtrial", ["登记号", "试验通俗题目", "试验专业题目", "适应症", "药物名称"])

    try:
        es_idx.delete(index="chictr_main")
    except:
        pass
    build(1000, "chictr_main", ["id", "title", "title_simple", "title_sci_name", "target_disease"])

    try:
        es_idx.delete(index="ctgov")
    except:
        pass
    build(1000, "ctgov", ["ct_id", "brief_title", "official_title", "ct_condition", "intervention"])

    # 大会Event：manual_event
    try:
        es_idx.delete(index="manual_event")
    except:
        pass
    build(2000, "manual_event", ["Record_ID", "Event_Name", "Reports", "Session_Name"])

    # NSFC：
    try:
        es_idx.delete(index="nsfc_data")
    except:
        pass
    build(2000, "nsfc_data", ["applyCode", "name", "keywordCh", "keywordEn"])

    # 学会：manual_association
    try:
        es_idx.delete(index="manual_association")
    except:
        pass
    build(2000, "manual_association", ["Record_ID", "Asso_Name", "Field"])

    # 指南：manual_guide
    try:
        es_idx.delete(index="manual_guide")
    except:
        pass
    build(1000, "manual_guide", ["Record_ID", "Guide_Name", "Field"])

import sys
import json
import pandas as pd
import numpy as np
import re
import string
import jieba
import collections
import pickle
import codecs
import pymysql
import mysql.connector as sql

def str_2_utf8(str):

    try:
        return str.decode("utf-8")
    except:
        pass

    try:
        return str.decode("gbk")
    except:
        pass

    return str

if __name__ == "__main__":

    input_json = {"input_query_words": ["功能梯度材料", "耦合"], "input_keywords_dict": ["功能梯度材料", "耦合"]}
    # input_json = json.loads(sys.argv[1])
    query_words = input_json['input_query_words']         # 需要查询的关键词列表(一个或多个) list of string
    keywords_dict = input_json['input_keywords_dict']     # 用户提供的词典(一个或多个)，词典中每个词代表一个实体 list of string

    conn = pymysql.connect(host = 'rm-2zet5lw17as40fty28o.mysql.rds.aliyuncs.com',
                           port = 3306,
                           user = 'snowball',
                           passwd = 'MEDOsnow$%^&',
                           db = 'medo_master')
    cur = conn.cursor()

    sql_condition = ""
    sql_condition_template = "name LIKE '%%%s%%' or keywordCH like '%%%s%%' or keywordEn like '%%%s%%' or summaryCH like '%%%s%%' or comSummary like '%%%s%%'"
    for idx, word in enumerate(query_words):
        if idx == 0:
            sql_condition += sql_condition_template % (word, word, word, word, word)
        else:
            sql_condition += " or " + sql_condition_template % (word, word, word, word, word)

    sql_command = "SELECT * FROM nsfc_data WHERE " + sql_condition
    print('SQL: ', sql_command)

    cur.execute(sql_command)
    res = cur.fetchall()
    res_df = pd.DataFrame(list(res), columns=['applyCode', 'category', 'categoryCode', 'code', 'comSummary',
                                              'fundedYearEnd', 'fundedYearStart', 'funding', 'keywordCh',
                                              'keywordEn', 'leader', 'leaderCode', 'leaderTitle', 'name',
                                              'organization', 'organizationCode', 'outComeAwardNum',
                                              'outComeConferenceNum', 'outComeJournalNum', 'outComePatent',
                                              'outComeWorkNum', 'searchYearEnd', 'summaryCh', 'summaryEn',
                                              'tranceNo', 'DID'])










    # res_df.to_csv('./demo_search_res.csv')





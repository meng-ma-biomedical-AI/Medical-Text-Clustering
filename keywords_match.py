import sys
import json
import pandas as pd
import numpy as np
import re
import time
import string
import jieba
import pickle
import codecs
import pymysql
import mysql.connector as sql
from tempfile import NamedTemporaryFile

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

def jieba_cut(text, user_dict):
    if not text:
        text = ""
    with NamedTemporaryFile('w+t', suffix='.txt') as f:
        f.write("\n".join(user_dict))
        # print(f.name)
        jieba.load_userdict(f.name)
        return list(jieba.cut(text, cut_all=False))


if __name__ == "__main__":

    time_1 = time.time()

    input_json = {"input_query_words": ["毒性"], "input_keywords_dict": ["毒性", "心脏毒性"]}
    # input_json = json.loads(sys.argv[1])
    query_words = input_json['input_query_words']         # 需要查询的关键词列表(一个或多个) list of string
    keywords_dict = input_json['input_keywords_dict']     # 用户提供的词典(一个或多个)，词典中每个词代表一个实体 list of string

    conn = pymysql.connect(host = 'rm-2zet5lw17as40fty28o.mysql.rds.aliyuncs.com',
                           port = 3306,
                           user = 'snowball',
                           passwd = 'MEDOsnow$%^&',
                           db = 'medo_master')
    cur = conn.cursor()

    print(jieba_cut('心脏毒性是一种病。', keywords_dict))

    sql_condition = ""
    sql_condition_template = "name LIKE '%%%s%%' or keywordCh like '%%%s%%' or keywordEn like '%%%s%%' or summaryCh like '%%%s%%' or comSummary like '%%%s%%'"
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
    print('aaaaa: ', len(res_df))

    text_columns = ['name', 'keywordCh', 'keywordEn', 'summaryCh', 'comSummary']
    drop_idx = []
    with NamedTemporaryFile('w+t', suffix='.txt') as f:
        f.write("\n".join(keywords_dict))
        # print(f.name)
        jieba.load_userdict(f.name)
        for idx, row in res_df.iterrows():
            contain_word = True
            for word in query_words:
                if not contain_word:
                    drop_idx.append(idx)
                    break
                contained_in_one_column = False
                for col in text_columns:
                    if contained_in_one_column:
                        break
                    print(jieba_cut('心脏毒性是一种病。', keywords_dict))
                    col_text_list = [] if not row[col] else list(jieba.cut(row[col], cut_all=False))
                    contained_in_one_column |= word in col_text_list
                contain_word &= contained_in_one_column

    res_df.drop(drop_idx, inplace=True)

    print('bbbbb: ', len(res_df))

    # res_df.to_csv('./demo_search_res.csv')

    print('time: ', time.time() - time_1)




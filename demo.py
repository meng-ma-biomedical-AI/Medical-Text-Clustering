import pandas as pd
import numpy as np
import re
import string
import jieba
import collections
import pickle
import codecs
import mysql.connector as sql

mesh = pd.read_csv('./data/a.csv')
mesh.dropna(axis=0, inplace=True, subset=['translated'])
mesh_zh_dict = mesh['translated'].tolist()
mesh_zh_dict = list(set(mesh_zh_dict))
print(len(mesh_zh_dict))

chinese_word_dict = pickle.load(open('./dict data/chinese_word_dict.pickle', 'rb'))
print(len(chinese_word_dict))
chinese_word_dict += mesh_zh_dict
chinese_word_dict = list(set(chinese_word_dict))
print(len(chinese_word_dict))

jieba.load_userdict('./dict data/chinese_word_dict.txt')
stopwords = [line.strip() for line in codecs.open('./stopwords-zh.txt', 'r', encoding='utf8').readlines()]

# all_question = pd.read_csv('./data/description.csv')
Herceptin_question = pd.read_excel('./data/Herceptin.xlsx')
# all_question = pd.read_excel('./data/Herceptin_funding_filtered.xlsx')
# all_question = pd.read_excel('./data/Herceptin_patient_filtered.xlsx')
Kadcyla_question = pd.read_excel('./data/Kadcyla.xlsx')
Perjeta_1_question = pd.read_excel('./data/Perjeta_1.xlsx')
Perjeta_2_question = pd.read_excel('./data/Perjeta_2.xlsx')
Tecentriq_question = pd.read_excel('./data/Tecentriq.xlsx')
Xeloda_question = pd.read_excel('./data/Xeloda.xlsx')
No_product_question = pd.read_excel('./data/No_product_merged.xlsx')

all_question = pd.concat([Herceptin_question, Kadcyla_question, Perjeta_1_question, Perjeta_2_question, Tecentriq_question, Xeloda_question, No_product_question])
print('all_question: ', len(all_question))
all_question.head(5)

all_question.dropna(axis=0, inplace=True, subset=['Description'])
all_question.isna().sum()

print('all_question: ', len(all_question))


import jieba.analyse.textrank as textrank
jieba.analyse.set_stop_words("./stopwords-zh.txt")

def keyword_extract(text, word_dict):
#     text = list(jieba.cut(text, cut_all=False))
#     text = list(jieba.cut_for_search(text))
#     text = [w for w in text if w in word_dict]
    return " ".join(textrank(text))

# 去除所有“[]”内的文本
def remove_special_string(text):
    text = re.sub(r'\[[^]]*\]', '', text)
    text = re.sub(r'\([^)]*\)', '', text)
    text = re.sub(r'（[^）]*\）', '', text)
    text = re.sub(r'\<[^>]*\>', '', text)
    return text

def remove_stopwords(text, stopwords):
    keywords = text.split()
    keywords = [w for w in keywords if w not in stopwords]
    return "  ".join(keywords)

# all_question['Description'] = all_question.Description.apply(lambda x: remove_special_string(x))
# all_question['KeyWords'] = all_question.Description.apply(lambda x: keyword_extract(x, chinese_word_dict))
# all_question['KeyWords'] = all_question.KeyWords.apply(lambda x: remove_stopwords(x, stopwords))

print(keyword_extract("新辅助化疗有效或者无效的蛋白质组有什么区别？", chinese_word_dict))

# description_list = all_question['Description'].tolist()
# keywords_list = all_question['KeyWords'].tolist()
# print(len(description_list))
# print(len(keywords_list))
# write_list = []
# for idx, item in enumerate(description_list):
#     line = '原文本： ' + item + '\n' + '关键词： ' + keywords_list[idx] + '\n\n'
#     write_list.append(line)

# f_sourcetext_and_keywords = open('text_keywords_4.txt', 'w')
# f_sourcetext_and_keywords.writelines(write_list)
# f_sourcetext_and_keywords.close()
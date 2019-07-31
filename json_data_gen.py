import json
import pymysql

conn = pymysql.connect(host = 'rm-2zet5lw17as40fty28o.mysql.rds.aliyuncs.com',
                       port = 3306,
                       user = 'snowball',
                       passwd = 'MEDOsnow$%^&',
                       db = 'medo_master')
cur = conn.cursor()
sql_command = "SELECT pmid, title FROM pubmed_articles"

cur.execute(sql_command)
res = list(cur.fetchall())
res_count = len(res)
print(res_count)

with open('./json_data_pubmed_articles.json', 'w') as f:
    for idx, item in enumerate(res[:300000]):
        index_line = json.dumps({ "index" : { "_index" : "pubmed_articles", "_type" : "docs", "_id" : str(idx+1) } }) + '\n'
        data_line = json.dumps({"pmid": item[0], "title": item[1]}) + '\n'
        f.writelines([index_line, data_line])


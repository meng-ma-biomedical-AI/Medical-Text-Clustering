import sys
from ES_keywords_search import *

if __name__ == '__main__':

    keywords = sys.argv[1]
    search_table = sys.argv[2]
    search_fields = sys.argv[3]

    search_results = query_keyword(search_table, search_fields, keywords)

    print(search_results)
    print(len(search_results))
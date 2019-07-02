import sys
from elasticsearch import Elasticsearch

es = Elasticsearch([{'host':'localhost','port':9200}])

query = sys.argv[1]

# processed_text  pure_text
results = es.search(index='main_idx', body=
{
  "query" : { "match" : { "pure_text" : query }}
}
,size=10)
hits = results['hits']['hits']
# results
for idx, res in enumerate(hits):
    print(str(idx+1) + ":  " + res['_source']['pure_text'])

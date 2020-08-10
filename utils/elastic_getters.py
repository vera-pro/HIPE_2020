from utils.data_processing import extract_entity_mentions
import pandas as pd

# Get label by its Q-value
def wikidata_get_label(es, label_exact, index_="wikidata_clef"):
    query = {
        "query": {
            "multi_match" : {
                "query" : label_exact,
               "fields" : ["label_exact"]
            }
        }
    }

    res = es.search(index=index_, body=query)
    return res['hits']['hits']

# Get description by a Q-value
def wikidata_get_description(es, label_exact, index_="wikidata_descriptions"):
    query = {
        "query": {
            "multi_match" : {
                "query" : label_exact,
               "fields" : ["label_exact"]
            }
        }
    }

    res = es.search(index=index_, body=query)
    return res['hits']['hits']

# Fuzzy matching for OCR mistakes
def wikidata_search_fuzzy(es, term, index_="wikidata_clef"):   
    query = {
         "query": {
            "bool": {
              "must": {
                "multi_match": {
                    "query" : term,
                    "fields" : ["label.snowball", "label.precise"], #, "label.ngram"],
                    "fuzziness": "AUTO"
                }
              },
              "filter": {
                "wildcard": {
                  "label_exact": "Q*"
                }
              }
            }
          }
    }

    res = es.search(index=index_, body=query)
    return res['hits']['hits'] 

# Search for an entity: precise matching
def wikidata_search_precise(es, term, index_="wikidata_clef"):    
    query = {
         "query": {
             "bool": {
                "must": {
                    "term": {
                        "label.raw" : term.lower(),
                    }
                },
                "filter": {
                    "wildcard": {
                      "label_exact": "Q*"
                    }
                }
            }
          },
        "sort": [
            { "count": {"order":"desc"}}
        ]
    }

    res = es.search(index=index_, body=query)

    return res['hits']['hits'] 

# Full-text search
def wikidata_search_(es, term, index_="wikidata_clef"):   
    query = {
         "query": {
            "bool": {
              "must": {
                "multi_match": {
                    "query" : term,
                    "fields" : ["label.snowball", "label.precise", "label.ngram"],
                    "type": "phrase"
                }
              },
              "filter": {
                "wildcard": {
                  "label_exact": "Q*"
                }
              }
            }
          }
    }

    res = es.search(index=index_, body=query)
    return res['hits']['hits'] 

# Search for an entity
def get_wikidata_entries(es, entity):
    hits = wikidata_search_(es, entity)
    hits_exact = wikidata_search_precise(es, entity)
    
    if len(hits) + len(hits_exact) == 0:
        return "NIL"
    
    answer_length = min(5, len(hits_exact))
    results = [hit['_source']['label_exact'] for hit in hits_exact[:answer_length]]
    remaining_length = 5 - answer_length
    
    if remaining_length > 0:
        results.append('NIL')
        remaining_length -= 1
        
    remaining_hits = [hit for hit in hits if hit['_source']['label_exact'] not in results]
    

    if remaining_length > 0:
        remaining_hits_sorted = sorted(remaining_hits, key=lambda x: (-x['_score'], len(x['_source']['label_exact']), x['_source']['count']))
        cnt = 0
        while remaining_length > 0 and cnt < len(remaining_hits_sorted):
            results.append(remaining_hits_sorted[cnt]['_source']['label_exact'])
            cnt += 1
            remaining_length -= 1
            
    return '|'.join(results)


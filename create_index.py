from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Index
import json,re,os
import datetime

client = Elasticsearch(HOST="http://localhost",PORT=9200)
INDEX = 'song-index1'


def createIndex():
    settings = {
        "settings": {
            "index":{
                "number_of_shards": "1",
                "number_of_replicas": "1"
            },
            "analysis" :{
                "analyzer":{
                    "sinhala-analyzer":{
                        "type": "custom",
                        "tokenizer": "icu_tokenizer",
                        "filter":["edge_ngram_custom_filter"]
                    },
                
                },
                "filter" : {
                    "edge_ngram_custom_filter":{
                        "type": "edge_ngram",
                        "min_gram" : 2,
                        "max_gram" : 50,
                        "side" : "front"
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                    "title": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        },
                        "analyzer" : "sinhala-analyzer",
                        "search_analyzer": "standard"
                    },
                    "lyrics": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        },
                        "analyzer" : "sinhala-analyzer",
                        "search_analyzer": "standard"
                    },
                    "artist": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        },
                        "analyzer" : "sinhala-analyzer",
                        "search_analyzer": "standard"
                    },
                    "lyricist": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        },
                        "analyzer" : "sinhala-analyzer",
                        "search_analyzer": "standard"
                    },
                    "album": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        },
                        "analyzer" : "sinhala-analyzer",
                        "search_analyzer": "standard"
                    },
                    "year": {
                        "type": "integer",
                    },
            }
        }
    }


    # index = Index(INDEX,using=client)
    # result = index.create()
    result = client.indices.create(index=INDEX , body =settings)
    print (result)

def read_all_songs():
    with open('songs_corpus/sinhala_songs_metaphor_corpus_180126p.json', 'r', encoding='utf8') as f:
        tra_songs = json.loads(f.read())
        results_list = [a for num, a in enumerate(tra_songs) if a not in tra_songs[num + 1:]]
        return results_list


def data_generation(song_array):
    for song in song_array:

        title = song["title"]
        #song_lyrics = clean_function(song["song_lyrics"])
        artist = song["artist"]
        lyricist = song["lyricist"]
        album = song["album"]
        year = datetime.datetime(int(song["year"]), 1, 1).year
        lyrics = song["lyrics"]
        metaphors = song["metaphors"]
        
        yield {
            "_index": INDEX,
            "_source": {
                "title": title,
                "artist": artist,
                "lyricist": lyricist,
                "album": album,
                "year": year,
                "lyrics": lyrics,
                "metaphors": metaphors
            },
        }



# createIndex()
translated_songs = read_all_songs()
helpers.bulk(client,data_generation(translated_songs))
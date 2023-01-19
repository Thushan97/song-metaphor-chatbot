import json

#best_fields
def multi_match_agg_best_fields(query, fields=['title','lyrics']):
	print ("QUERY FIELDS")
	print (fields)
	q = {
		"size": 100,
		"explain": True,
		"query": {
			"multi_match": {
				"query": query,
				"fields": fields,
				"operator": 'or',
				"type": "best_fields"
			}
		},
		"aggs": {
			"Title Filter": {
				"terms": {
					"field": "title.keyword",
					"size": 10
				}
			},
			"Lyricist Filter": {
				"terms": {
					"field": "lyricist.keyword",
					"size": 10
				}
			},
			"Artist Filter": {
				"terms": {
					"field": "artist.keyword",
					"size": 10
				}
			},
			"Lyrics Filter": {
				"terms": {
					"field": "lyrics.keyword",
					"size": 10
				}
			},
            "Album Filter": {
				"terms": {
					"field": "album.keyword",
					"size": 10
				}
			},
            "Metaphor Filter": {
				"terms": {
					"field": "metaphors.keyword",
					"size": 10
				}
			}
		}
	}

	q = json.dumps(q)
	return q

#cross_fields
def multi_match_agg_cross_fields(query, fields=['title','song_lyrics']):
    print ("QUERY FIELDS")
    print (fields)
    q = {
		"size": 100,
		"explain": True,
		"query": {
			"multi_match": {
				"query": query,
				"fields": fields,
				"operator": 'or',
				"type": "cross_fields"
			}
		},
		"aggs": {
			"Title Filter": {
				"terms": {
					"field": "title.keyword",
					"size": 10
				}
			},
			"Lyricist Filter": {
				"terms": {
					"field": "lyricist.keyword",
					"size": 10
				}
			},
			"Artist Filter": {
				"terms": {
					"field": "artist.keyword",
					"size": 10
				}
			},
			"Lyrics Filter": {
				"terms": {
					"field": "lyrics.keyword",
					"size": 10
				}
			},
            "Album Filter": {
				"terms": {
					"field": "album.keyword",
					"size": 10
				}
			},
            "Metaphor Filter": {
				"terms": {
					"field": "metaphors.keyword",
					"size": 10
				}
			}
		}
	}

    q = json.dumps(q)
    return q

# sort best
def multi_match_agg_sort_best(query, year, fields=['title','song_lyrics']):
    print ('sort year is ',year)
    q = {
		"size": 100,
		"sort": [
			{"year": {"order": "desc"}},
		],
		"query": {
			"multi_match": {
				"query": query,
				"fields": fields,
				"operator": 'or',
				"type": "best_fields"
			}
		},
		"aggs": {
			"Title Filter": {
				"terms": {
					"field": "title.keyword",
					"size": 10
				}
			},
			"Lyricist Filter": {
				"terms": {
					"field": "lyricist.keyword",
					"size": 10
				}
			},
			"Artist Filter": {
				"terms": {
					"field": "artist.keyword",
					"size": 10
				}
			},
			"Lyrics Filter": {
				"terms": {
					"field": "lyrics.keyword",
					"size": 10
				}
			},
            "Album Filter": {
				"terms": {
					"field": "album.keyword",
					"size": 10
				}
			},
            "Metaphor Filter": {
				"terms": {
					"field": "metaphors.keyword",
					"size": 10
				}
			}
		}
	}
    q = json.dumps(q)
    return q

# sort cross
def multi_match_agg_sort_cross_fields(query, year, fields=['title','lyrics']):
	print ('sort num is ',year)
	q = {
		"size": 100,
		"sort": [
			{"year": {"order": "desc"}},
		],
		"query": {
			"multi_match": {
				"query": query,
				"fields": fields,
				"operator": 'or',
				"type": "cross_fields"
			}
		},
		"aggs": {
			"Title Filter": {
				"terms": {
					"field": "title.keyword",
					"size": 10
				}
			},
			"Lyricist Filter": {
				"terms": {
					"field": "lyricist.keyword",
					"size": 10
				}
			},
			"Artist Filter": {
				"terms": {
					"field": "artist.keyword",
					"size": 10
				}
			},
			"Lyrics Filter": {
				"terms": {
					"field": "lyrics.keyword",
					"size": 10
				}
			},
            "Album Filter": {
				"terms": {
					"field": "album.keyword",
					"size": 10
				}
			},
            "Metaphor Filter": {
				"terms": {
					"field": "metaphors.keyword",
					"size": 10
				}
			}
		}
	}
	q = json.dumps(q)
	return q



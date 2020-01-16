from pyspark import SparkConf, SparkContext
import sys
import re

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('wikipedia popular')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

INDEX_datetime = 0;
INDEX_language = 1;
INDEX_pagename = 2;
INDEX_views = 3;
INDEX_data = 4;


def get_key(kv):
    return kv[0]

def get_value(kv):
    return kv[1]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

def get_tuple(line):                      # INPUT: 20160801-120000 en Unpopular_Page 1 200
    array_wiki = re.split('\s+',line)      # OUTPUT: tuple(20160801-120000,en,Unpopular_Page,1,200)
    return (array_wiki[INDEX_datetime],array_wiki[INDEX_language],array_wiki[INDEX_pagename],int(array_wiki[INDEX_views]), array_wiki[INDEX_data])

def create_keyval_pair(line):
    return (line[INDEX_datetime],(line[INDEX_views],line[INDEX_pagename]))
 #   return (line[INDEX_datetime],line[INDEX_views])

def get_max(x,y):
    return max(x,y)

def tab_separated(kv):
    return  "%s\t%s" % (kv[0], kv[1])

text = sc.textFile(inputs)
words = text.map(get_tuple)
filtered_words = words.filter(lambda words: words[INDEX_language]=='en' and words[INDEX_pagename] !='Main_Page'and not words[INDEX_pagename].startswith('Special:'))
keyval_pair = filtered_words.map(create_keyval_pair)
max_view = keyval_pair.reduceByKey(get_max)

outdata = max_view.sortBy(get_value)
outdata = outdata.map(tab_separated)
outdata.saveAsTextFile(output)

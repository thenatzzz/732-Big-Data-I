from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import re,string

# add more functions as necessary
def words_once(line):
    wordsep =re.compile(r'[%s\s]+' % re.escape(string.punctuation))
    for w in wordsep.split(line):
        if w: # Empty string is falsy; therefore, we only accept non-empty string here.
            yield (w.lower(), 1)
def add(x, y):
    return x + y
def get_key(kv):
    return kv[0]
def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

def main(inputs, output):
    # main logic starts here
    text = sc.textFile(inputs)
    text = text.repartition(20)
    words = text.flatMap(words_once)
    wordcount = words.reduceByKey(add)

    outdata = wordcount.sortBy(get_key).map(output_format)
    outdata.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('word count improved')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)


''' OLD_CODE
from pyspark import SparkConf, SparkContext
import sys
import re,string

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('word count')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

def words_once(line):
    wordsep =re.compile(r'[%s\s]+' % re.escape(string.punctuation))	
    for w in wordsep.split(line):
        if w: # Empty string is falsy; therefore, we only accept non-empty string here.
            yield (w.lower(), 1)
def add(x, y):
    return x + y

def get_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

text = sc.textFile(inputs)
words = text.flatMap(words_once)
wordcount = words.reduceByKey(add)

outdata = wordcount.sortBy(get_key).map(output_format)
outdata.saveAsTextFile(output)
'''

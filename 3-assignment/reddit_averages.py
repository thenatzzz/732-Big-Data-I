
from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json

# add more functions as necessary
def create_keyval_pair(single_dict):
    return (single_dict['subreddit'],(1,single_dict['score']))

def add_pairs(x,y):
    return (x[0]+y[0],x[1]+y[1])

def cal_average_score(single_pair):
    return (single_pair[0], single_pair[1][1]/single_pair[1][0])

def main(inputs, output):
    # main logic starts here
    RDD_strings = sc.textFile(inputs)
    RDD_dict = RDD_strings.map(json.loads)
    keyval_pair = RDD_dict.map(create_keyval_pair)
    print(keyval_pair.take(5) , " ****************************")
    reduced_pair = keyval_pair.reduceByKey(add_pairs)
    print(reduced_pair.take(5), " -------------------------")
    final_pair = reduced_pair.map(cal_average_score)
    print(final_pair.take(5), " ++++++++++++++++++++++++++++++")
    output_json = final_pair.map(json.dumps)
    output_json.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit averages')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

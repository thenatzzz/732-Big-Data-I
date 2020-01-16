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

def get_average_positive(sub_avgscore):
    subreddit = sub_avgscore[0]
    avg_score = sub_avgscore[1]
    if avg_score > 0:
        return (subreddit, avg_score) 

def get_subreddit_comment_pair(single_dict):
    return (single_dict['subreddit'],single_dict)

def create_author_relative_score_pair(big_single_dict):
    subreddit = big_single_dict[0]
    avgscore_comment_pair = big_single_dict[1] 
    avg_score = avgscore_comment_pair[0]
    whole_comment = avgscore_comment_pair[1]
    return (whole_comment['author'], whole_comment['score']/avg_score)	

def main(inputs, output):
    # main logic starts here
    RDD_strings = sc.textFile(inputs)
    RDD_dict = RDD_strings.map(json.loads)
    keyval_pair = RDD_dict.map(create_keyval_pair)
    reduced_pair = keyval_pair.reduceByKey(add_pairs)
    sub_pos_avg_pair = reduced_pair.map(cal_average_score).filter(get_average_positive) 
    
    RDD_dict2 = RDD_strings.map(json.loads).cache()
    subreddit_comment_pair = RDD_dict2.map(get_subreddit_comment_pair)
    subreddit_comment_with_avg_score = sub_pos_avg_pair.join(subreddit_comment_pair) 
    author_relative_score_pair = subreddit_comment_with_avg_score.map(create_author_relative_score_pair).sortBy(ascending=False,keyfunc=lambda x: x[1])
    #print(author_relative_score_pair.take(5))
    output_json = author_relative_score_pair.map(json.dumps).saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit relative score')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

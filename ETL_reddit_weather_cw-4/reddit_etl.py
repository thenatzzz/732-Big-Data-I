
from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json

# add more functions as necessary
def keep_sub_score_author(single_dict):	
    return (single_dict['subreddit'], single_dict['score'], single_dict['author'])

def remove_comment_w_e(sub_score_author):
    subreddit = sub_score_author[0]
    score = sub_score_author[1]
    author = sub_score_author[2]
    single_letter = 'e'
    if single_letter not in subreddit:
        return (subreddit,score,author)

def get_score_positive(sub_score_author):
    subreddit = sub_score_author[0]
    score = sub_score_author[1]
    author = sub_score_author[2]
    if score > 0:
        return (subreddit,score,author)

def get_score_negative(sub_score_author):
    subreddit = sub_score_author[0]
    score = sub_score_author[1]
    author = sub_score_author[2]
    if score <= 0:
        return (subreddit,score,author)

def main(inputs, output):
    # main logic starts here
    RDD_strings = sc.textFile(inputs)
    RDD_dict = RDD_strings.map(json.loads)
    sub_score_author = RDD_dict.map(keep_sub_score_author)
    filtered_sub_score_author = sub_score_author.filter(remove_comment_w_e)
   # filtered_sub_score_author = sub_score_author.filter(remove_comment_w_e).cache()
    sub_pos_score_author = filtered_sub_score_author.filter(get_score_positive).map(json.dumps).saveAsTextFile(output+'/positive')
    sub_neg_score_author = filtered_sub_score_author.filter(get_score_negative).map(json.dumps).saveAsTextFile(output+'/negative')
   
if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit averages etl')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

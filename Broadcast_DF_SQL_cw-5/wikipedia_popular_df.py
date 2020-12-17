#from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json
from pyspark.sql import SparkSession,functions,types
from pyspark.sql.types import *
from pyspark.sql.functions import *
from textwrap import wrap

spark = SparkSession.builder.appName('wikipedia popular df').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext  

# add more functions as necessary
@functions.udf(returnType=types.StringType())
def path_to_hour(path):
    ignored_path_len = len("hdfs://nml-cloud-149.cs.sfu.ca:8020/courses/732/pagecounts-0/pagecounts-")
    #### textwrap library is very slow for large dataset. !! ###
    #splitted_path = wrap(path,ignored_path_len)
    #ignored_path = splitted_path[0]
    #filename = splitted_path[1]
    ###########################################################
    filename = path[ignored_path_len:]    
    datehour_len = len("20160801-12")
    return filename[:datehour_len]

   
def main(inputs, output):
    # main logic starts here
    comments_schema = types.StructType([
    types.StructField('language', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('views', types.LongType()),
    types.StructField('content_size', types.LongType())
    ])

    original_input = spark.read.csv(inputs, schema=comments_schema,sep=" ").withColumn('hour',functions.input_file_name())
    input_with_filename = original_input.withColumn('hour',path_to_hour(original_input['hour'])).cache()
 
    wikipedia_popular = input_with_filename.where(input_with_filename['language'] == 'en')
    wikipedia_popular = wikipedia_popular.where( wikipedia_popular['title'] !='Main_Page')
    wikipedia_popular = wikipedia_popular.where( wikipedia_popular['title'].startswith('Special:')!= True) 
       
    max_views_per_hour = wikipedia_popular.groupby('hour').agg(functions.max(wikipedia_popular['views']).alias('max_views')).cache()
       
    hour_title_view = input_with_filename.select(input_with_filename['hour'],input_with_filename['title'],input_with_filename['views'])  
    print(' length of max_views data:',max_views_per_hour.count())
    print(' length of original data:', hour_title_view.count())
    
    hour_title_maxview = hour_title_view.join((max_views_per_hour),((hour_title_view['views'] == max_views_per_hour['max_views']) & (hour_title_view['hour']==max_views_per_hour['hour'])),'left_semi')
    
    sorted_hour_title_maxview = hour_title_maxview.sort(desc("hour"))
    print(sorted_hour_title_maxview.show(10))
    sorted_hour_title_maxview.explain() 
    sorted_hour_title_maxview.write.csv(output,mode='overwrite')
    
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)


from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
import re
import math
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import uuid
from datetime import datetime
from pyspark.sql.functions import udf

# add more functions as necessary

line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

def input_filter(single_rdd):
    return line_re.match(single_rdd)
def input_preprocess(single_rdd):
    new_list = line_re.split(single_rdd)
    new_list = list(filter(None,new_list))    
    hostname = new_list[0]
    date_time = datetime.strptime(new_list[1],'%d/%b/%Y:%H:%M:%S')
    path = new_list[2]
    byte = int(new_list[3])    
    return [hostname,byte,date_time,path] 

def main(inputs,keyspace,table):
    # main logic starts here    
    RDD_original = sc.textFile(inputs).repartition(200)
    rdd = RDD_original.filter(input_filter)
    rdd = rdd.map(input_preprocess)
    
    sqlContext = SQLContext(sc)
    df =sqlContext.createDataFrame(rdd,['host','bytes','datetime','path'])
    uuidUdf= udf(lambda : str(uuid.uuid4()),StringType())
    df1 = df.withColumn('uuid',uuidUdf()) 
    df1.write.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=keyspace).save()
    df1 = spark.read.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=keyspace).load()
    

if __name__ == '__main__':
    conf = SparkConf().setAppName('nasa log')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output_keyspace = sys.argv[2]
    table_name = sys.argv[3]
    
    cluster_seeds = ['199.60.17.32', '199.60.17.65']
    spark = SparkSession.builder.appName('Spark Cassandra example').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    main(inputs,output_keyspace,table_name)

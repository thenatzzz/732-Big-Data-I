
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
import math
from pyspark.sql.types import *

# add more functions as necessary
def calculate_r(n,x,y,x2,y2,xy):
    numerator = (n*xy)-(x*y)
    denom1 =  math.sqrt( (n*x2) - math.pow(x,2) )
    denom2 =  math.sqrt( (n*y2) - math.pow(y,2) )
    return numerator/(denom1*denom2)


def main(keyspace,table):
    # main logic starts here    
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=keyspace).load()
    df = df.withColumn('count',functions.lit(1))
    host_count_byte = df.groupBy('host').agg({'count':'sum','bytes':'sum'}).withColumnRenamed('SUM(count)','x_count').withColumnRenamed('SUM(bytes)','y_byte')

    total_num_data =host_count_byte.count()

    result = host_count_byte.withColumn('x2_count',host_count_byte['x_count']**2)
    result = result.withColumn('y2_byte',result['y_byte']**2)
    result = result.withColumn('xy',result['x_count']*result['y_byte'])

    x_sum = result.groupBy().agg({'x_count':'sum'}).collect()[0]['sum(x_count)']
    y_sum = result.groupBy().agg({'y_byte':'sum'}).collect()[0]['sum(y_byte)']
    y2_sum = result.groupBy().agg({'y2_byte':'sum'}).collect()[0]['sum(y2_byte)']
    x2_sum =  result.groupBy().agg({'x2_count':'sum'}).collect()[0]['sum(x2_count)']
    xy_sum =  result.groupBy().agg({'xy':'sum'}).collect()[0]['sum(xy)']
    print("x_sum",x_sum)
    print("y_sum",y_sum)
    print("x2_sum",x2_sum)
    print("y2_sum",y2_sum)
    print("xy_sum",xy_sum)

    result.show(10)

    r_val = calculate_r(total_num_data,x_sum,y_sum,x2_sum,y2_sum,xy_sum)
    print("r = ", r_val)
    print("r^2 =",math.pow(r_val,2))
    
    ''' code to compare with built-in library
    host_count_byte = host_count_byte.withColumn('x_count',host_count_byte['x_count'].cast(FloatType()))
    host_count_byte = host_count_byte.withColumn('y_byte',host_count_byte['y_byte'].cast(FloatType()))
    print("r with corr library = ",host_count_byte.stat.corr('x_count','y_byte'))
    '''

if __name__ == '__main__':
   
    output_keyspace = sys.argv[1]
    table_name = sys.argv[2]
    
    cluster_seeds = ['199.60.17.32', '199.60.17.65']
    spark = SparkSession.builder.appName('Spark Cassandra nasalogs').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    main(output_keyspace,table_name)

    assert spark.version >= '2.4'  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext


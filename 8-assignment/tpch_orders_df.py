
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.types import *

# add more functions as necessary
def output_line(single_rdd):
    orderkey = single_rdd[0]
    price = single_rdd[1]
    names = single_rdd[2]
    namestr = ', '.join(sorted(list(names)))
    return 'Order #%d $%.2f: %s' % (orderkey, price, namestr)

def main(keyspace,outdir,orderkeys):
    # main logic starts here
    # read each table from Cassandra into dataframe
    tb_orders = 'orders'    
    df_orders = spark.read.format("org.apache.spark.sql.cassandra").options(table=tb_orders, keyspace=keyspace).load()
    tb_part = 'part'
    df_part = spark.read.format("org.apache.spark.sql.cassandra").options(table=tb_part, keyspace=keyspace).load()
    tb_lineitem = 'lineitem'
    df_lineitem = spark.read.format("org.apache.spark.sql.cassandra").options(table=tb_lineitem, keyspace=keyspace).load()

    # create SQL table in order to use SQL-like commands
    df_orders.createOrReplaceTempView('orders')
    df_part.createOrReplaceTempView('part')
    df_lineitem.createOrReplaceTempView('lineitem')
    
    table = spark.sql("""SELECT o.*, p.name 
                          FROM orders o 
                           JOIN lineitem l ON l.orderkey = o.orderkey 
                         JOIN part p ON p.partkey = l.partkey 
			  WHERE (o.orderkey = l.orderkey 
				  AND l.partkey = p.partkey)""")
    
    final_table = table.select(functions.col('orderkey'),functions.round(table['totalprice'],2).alias('totalprice'),functions.col('name')).groupby('orderkey','totalprice').agg(functions.sort_array(functions.collect_set('name')).alias('name'))
    final_table = final_table.orderBy('orderkey')
    #print(final_table.show(10,False))
    
    # filter table according to of orderkeys list 
    filtered_final_table = final_table.where(final_table.orderkey.isin(orderkeys))
    final_table.where(final_table.orderkey.isin(orderkeys)).explain()
    #print(filtered_final_table.show(5,False))    
    
    # turn dataframe to RDD
    rdd = filtered_final_table.rdd.map(list)    
    output = rdd.map(output_line)
    print(output.take(5))
    output.saveAsTextFile(outdir)
    
if __name__ == '__main__':   
    keyspace = sys.argv[1]
    outdir = sys.argv[2]
    orderkeys = sys.argv[3:]
    cluster_seeds = ['199.60.17.32', '199.60.17.65']
    spark = SparkSession.builder.appName('Spark Cassandra tpch_orders df').config('spark.dynamicAllocation.maxExecutors', 16).config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '2.4'  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(keyspace,outdir,orderkeys)

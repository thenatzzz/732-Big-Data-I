import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.types import *

def main(keyspace_in,keyspace_out):
    # main logic starts here
    # read each table from Cassandra into dataframe
    tb_orders = 'orders'    
    df_orders = spark.read.format("org.apache.spark.sql.cassandra").options(table=tb_orders, keyspace=keyspace_in).load().cache()
    tb_part = 'part'
    df_part = spark.read.format("org.apache.spark.sql.cassandra").options(table=tb_part, keyspace=keyspace_in).load()
    tb_lineitem = 'lineitem'
    df_lineitem = spark.read.format("org.apache.spark.sql.cassandra").options(table=tb_lineitem, keyspace=keyspace_in).load()

    # create SQL table from dataframe in order to use SQL-like commands
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
    #get only orderkey and name list column from our final table
    final_table = final_table.select(functions.col('orderkey'),functions.col('name').alias('part_names'))
    
 
    final_table.createOrReplaceTempView('final_table_sql')
    df_orders.createOrReplaceTempView('orders')
    #select all columns from orders table and join them with part_names from final_table
    orders_parts = spark.sql("""SELECT o.*, f.part_names
                                FROM orders o 
                                JOIN final_table_sql f ON f.orderkey = o.orderkey
                                """)
    final_orders_parts = orders_parts.orderBy('orderkey')
    final_orders_parts.write.format("org.apache.spark.sql.cassandra") \
    .options(table='orders_parts', keyspace=keyspace_out).save()
    print("Successfully inserted!")

if __name__ == '__main__':   
    keyspace_in = sys.argv[1]
    keyspace_out = sys.argv[2]
    
    cluster_seeds = ['199.60.17.32', '199.60.17.65']
    spark = SparkSession.builder.appName('Spark Cassandra tpch').config('spark.dynamicAllocation.maxExecutors', 16).config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '2.4'  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(keyspace_in,keyspace_out)











import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.types import *
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

#  orderkey | clerk | comment | custkey | order_priority | orderdate | orderstatus | part_names | ship_priority | totalprice
#w(orderkey, clerk, comment, custkey, order_priority, orderdate, orderstatus, ship_priority, totalprice, part_names=]
IND_ORDERKEY = 0
def insert_batch(session,tpch):
    insert_user = session.prepare("""INSERT INTO orders_parts 
                                    (orderkey,clerk,comment,custkey,order_priority,orderdate,orderstatus,part_names,ship_priority,totalprice) 
                                   VALUES (?, ?,?,?,?,?,?,?,?,?)""")
    batch = BatchStatement()
    for single_tpch in tpch:
        orderkey = single_tpch[0]
        clerk = single_tpch[1]
        comment = single_tpch[2]
        custkey = single_tpch[3]
        order_priority = single_tpch[4]
        orderdate = single_tpch[5]
        orderstatus = single_tpch[6]
        part_names = set(single_tpch[9])
        ship_priority = single_tpch[7]
        totalprice = single_tpch[8]

        batch.add(insert_user, (orderkey,clerk,comment,custkey,order_priority,orderdate,orderstatus,part_names,ship_priority,totalprice ))
    session.execute(batch)
def create_total_list_rdd(single_rdd):
    return
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
    print(final_table.take(1))
 
    final_table.createOrReplaceTempView('final_table_sql')
    df_orders.createOrReplaceTempView('orders')
    orders_parts = spark.sql("""SELECT o.*, f.part_names
                                FROM orders o 
                                JOIN final_table_sql f ON f.orderkey = o.orderkey
                                """)
    final_orders_parts = orders_parts.orderBy('orderkey')
    #print("length: ",len(orders_parts.take(1)))
    final_orders_parts.write.format("org.apache.spark.sql.cassandra") \
    .options(table='orders_parts', keyspace=keyspace_out).save()
    '''
    rdd_orders_parts = orders_parts.rdd.map(list)
    #print("-------", rdd_orders_parts.take(2))    
    cluster = Cluster(['199.60.17.32', '199.60.17.65'])
    session = cluster.connect(keyspace_out)
    BATCH_VAL = 100
    
    list_rdd_orders_parts = rdd_orders_parts.map(lambda x: [x[IND_ORDERKEY],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9]]).collect()
    print(list_rdd_orders_parts[:2])
    for index in range(0,len(list_rdd_orders_parts),BATCH_VAL):
        #print(index, " ;;;; ", index+BATCH_VAL)
        insert_batch(session,list_rdd_orders_parts[index:index+BATCH_VAL])
    print(len(list_rdd_orders_parts),'length')
    '''
if __name__ == '__main__':   
    keyspace_in = sys.argv[1]
    keyspace_out = sys.argv[2]
    
    cluster_seeds = ['199.60.17.32', '199.60.17.65']
    spark = SparkSession.builder.appName('Spark Cassandra tpch').config('spark.dynamicAllocation.maxExecutors', 16).config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '2.4'  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(keyspace_in,keyspace_out)

'''
def insert_batch(session,logfile):
    insert_user = session.prepare("INSERT INTO nasalogs (host,uuid,bytes,datetime,path) VALUES (?, ?,?,?,?)")
    batch = BatchStatement()
    for single_log in logfile:
        host = single_log[0]
        date_time = datetime.strptime(single_log[1],'%d/%b/%Y:%H:%M:%S')
        path = single_log[2]
        byte = single_log[3]
        uuid_id = uuid.uuid4()
        batch.add(insert_user, (host,uuid_id,byte,date_time,path ))
    session.execute(batch)

def main(input_dir,keyspace,table_name):
    #print("input files: ", input_dir)
    logfile = load_data(input_dir)
    filtered_logfile = input_filter(logfile)
    clean_logfile = input_preprocess(filtered_logfile)
    #print("clean logfile: ",clean_logfile[:4])

    cluster = Cluster(['199.60.17.32', '199.60.17.65'])
    session = cluster.connect(keyspace)

    BATCH_VAL = 100

    for index in range(0,len(clean_logfile),BATCH_VAL):
        #print(index, " ;;;; ", index+BATCH_VAL)
        insert_batch(session,clean_logfile[index:index+BATCH_VAL])
    #print(len(clean_logfile),'length')
'''









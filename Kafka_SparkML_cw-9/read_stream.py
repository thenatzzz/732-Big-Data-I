import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName('Kafka readstream code').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

topic = sys.argv[1]
messages = spark.readStream.format('kafka') \
    .option('kafka.bootstrap.servers', '199.60.17.210:9092,199.60.17.193:9092') \
    .option('subscribe', topic).load()

messages.createOrReplaceTempView('messages_sql')
df_sql = spark.sql("SELECT CAST(value AS STRING) FROM messages_sql")

split_col = functions.split(df_sql['value'], ' ')
df_sql = df_sql.withColumn('x', split_col.getItem(0))
df_sql = df_sql.withColumn('y', split_col.getItem(1))
df_sql = df_sql.withColumn('xy', df_sql['x'] * df_sql['y'])
df_sql = df_sql.withColumn('x2', df_sql['x'] **2)

df_sql.createOrReplaceTempView('df_sql')
df_sql = spark.sql("""SELECT SUM(x) AS sum_x, 
                             SUM(y) AS sum_y, 
                             SUM(xy) AS sum_xy,
                             SUM(x2) AS sum_x2,
                             COUNT(*) AS num_count FROM df_sql""")
slope_beta_numerator = df_sql['sum_xy']-(df_sql['sum_x']*df_sql['sum_y']*(1/df_sql['num_count']))
slope_beta_denominator = df_sql['sum_x2']-(df_sql['sum_x']*df_sql['sum_x']*(1/df_sql['num_count']))
df_sql = df_sql.withColumn('slope_beta', slope_beta_numerator/slope_beta_denominator) 
df_sql = df_sql.withColumn('intercept_alpha', (df_sql['sum_y']*(1/df_sql['num_count']))-(df_sql['slope_beta']*df_sql['sum_x']*(1/df_sql['num_count'])))

query =  df_sql.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

query.awaitTermination(600)






















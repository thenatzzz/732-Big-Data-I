#Spark + DataFrames

import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('weather etl').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

# add more functions as necessary

def main(inputs, output):
    # main logic starts here
    observation_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.StringType()),
    types.StructField('observation', types.StringType()),
    types.StructField('value', types.IntegerType()),
    types.StructField('mflag', types.StringType()),
    types.StructField('qflag', types.StringType()),
    types.StructField('sflag', types.StringType()),
    types.StructField('obstime', types.StringType()),
])

    weather = spark.read.csv(inputs, schema=observation_schema)
    weather_onlynull_qflag = weather.filter(weather['qflag'].isNull()).cache()
   
    weather_t_min = weather_onlynull_qflag.filter(weather_onlynull_qflag['observation'].startswith('TMIN')).withColumnRenamed('observation','obs_TMIN').withColumnRenamed('value','value_TMIN')
    weather_t_max = weather_onlynull_qflag.filter(weather_onlynull_qflag['observation'].startswith('TMAX')).withColumnRenamed('observation','obs_TMAX').withColumnRenamed('value','value_TMAX').cache()
    
    weather_t_minmax = weather_t_max.join(weather_t_min,['date','station'])
    weather_t_minmax = weather_t_minmax.withColumn('full_range',weather_t_minmax['value_TMAX']-weather_t_minmax['value_TMIN'])
    weather_t_minmax = weather_t_minmax.withColumn('range',weather_t_minmax['full_range']/10.0)
    weather_t_minmax = weather_t_minmax.select(weather_t_minmax['date'],weather_t_minmax['station'],weather_t_minmax['range']).orderBy('date','station',ascending=True)
    print(weather_t_minmax.show(5))
    weather_t_minmax.write.csv(output,mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)


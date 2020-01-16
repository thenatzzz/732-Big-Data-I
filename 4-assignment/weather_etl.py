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
    print(weather.show(5))
    weather_onlynull_qflag = weather.filter(weather['qflag'].isNull())
    print(weather_onlynull_qflag.show(5))
    weather_ca = weather_onlynull_qflag.filter(weather_onlynull_qflag['station'].startswith('CA'))
    print(weather_ca.show(5))
    weather_obs_tmax = weather_ca.filter(weather_ca['observation'].startswith('TMAX'))
    print(weather_obs_tmax.show(5))
    weather_c = weather_obs_tmax.withColumn('tmax',weather_obs_tmax['value']/10.0)
    print(weather_c.show(5))
    weather_output = weather_c.select(weather_c['station'],weather_c['date'],weather_c['tmax'])
    print(weather_output.show(5))
    weather_output.write.json(output, compression='gzip', mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)


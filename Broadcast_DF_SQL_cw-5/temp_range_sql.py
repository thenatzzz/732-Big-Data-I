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

    weather = spark.read.csv(inputs, schema=observation_schema).cache()
    weather.createOrReplaceTempView('weather')
    weather_data = spark.sql("SELECT * from weather WHERE qflag IS NULL")
  #  print(weather_data.show(5))
    weather_data.createOrReplaceTempView('weather_data')
    
    weather_max = spark.sql("SELECT station,date,observation,value from weather_data WHERE observation LIKE 'TMAX%'").cache()
    weather_min = spark.sql("SELECT station,date,observation,value from weather_data WHERE observation LIKE 'TMIN%'")
    
    weather_max.createOrReplaceTempView("weather_max")
    weather_min.createOrReplaceTempView("weather_min")

    weather_minmax= spark.sql("""SELECT weather_max.station,
                                        weather_max.date,
                                        weather_max.observation,
                                        weather_max.value AS value_max,
                                        weather_min.station AS station_min,
                                        weather_min.date AS date_min,
                                        weather_min.observation,
                                        weather_min.value AS value_min
                                        FROM weather_max 
                                        INNER JOIN weather_min  
                                        ON (weather_max.station = weather_min.station 
                                            AND weather_max.date=weather_min.date)""") 
    #print(weather_minmax.show(5))  
    weather_minmax.createOrReplaceTempView('weather_minmax')    
    weather_range = spark.sql("""SELECT date, station,(value_max-value_min)/10.0 AS range
                                 FROM weather_minmax 
                                 ORDER BY date,station""")
    print(weather_range.show(10))
    weather_range.write.csv(output,mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)


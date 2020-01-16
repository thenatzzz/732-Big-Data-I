import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('weather prediction').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')
from pyspark.ml import PipelineModel
from datetime import datetime

#DataFrame[latitude: float, longitude: float, elevation: float, date: date,
# tmax: float, yesterday_tmax: float, day_of_year: int]
latitude = 49.2771
longitude = -122.9146
elevation = 330.0
date = '2019-Nov-08'
datetime_obj = datetime.strptime(date,'%Y-%b-%d')
tmax = 0.0
yesterday_tmax = 12.0

data_for_prediction = [latitude,longitude,elevation,datetime_obj,tmax,yesterday_tmax]

weather_schema = types.StructType([
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('date', types.DateType()),
    types.StructField('tmax', types.FloatType()),
    types.StructField('yesterday_tmax', types.FloatType()),
])

def predict(model_file):
    # load the model
    model = PipelineModel.load(model_file)
    df_for_prediction = spark.createDataFrame([data_for_prediction],schema=weather_schema )
    df_for_prediction = df_for_prediction.withColumn('day_of_year', functions.dayofyear('date'))

    # use the model to make predictions
    prediction_array = model.transform(df_for_prediction)
    prediction_array.show()
    prediction = prediction_array.select('prediction')
    print("Tmax for November 9,2019: ",prediction.collect()[0]['prediction'])

if __name__ == '__main__':
    model_file = sys.argv[1]
    predict(model_file)

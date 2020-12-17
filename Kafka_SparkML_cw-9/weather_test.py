import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('weather test').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')
from pyspark.sql import functions as fn
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import SQLTransformer

tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])

USE_YTD_TEMP_FEATURE = True

def test_model(model_file, inputs):
    # get the data
    test_tmax = spark.read.csv(inputs, schema=tmax_schema)
    #########################################################################
    if USE_YTD_TEMP_FEATURE:
        syntax = """SELECT today.latitude,today.longitude,today.elevation,today.date,
                           today.tmax, yesterday.tmax AS yesterday_tmax
                    FROM __THIS__ as today
                    INNER JOIN __THIS__ as yesterday
                    ON date_sub(today.date, 1) = yesterday.date
                       AND today.station = yesterday.station"""
        sql_trans = SQLTransformer(statement= syntax)
        test_tmax = sql_trans.transform(test_tmax)
    #######################################################################
    test_tmax = test_tmax.withColumn('day_of_year', fn.dayofyear('date'))
    # load the model
    model = PipelineModel.load(model_file)

    # use the model to make predictions
    predictions = model.transform(test_tmax)
    # predictions.show()

    # evaluate the predictions
    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',
            metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)

    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',
            metricName='rmse')
    rmse = rmse_evaluator.evaluate(predictions)

    print('r2 =', r2)
    print('rmse =', rmse)

    # If you used a regressor that gives .featureImportances, maybe have a look...
    print(model.stages[-1].featureImportances)


if __name__ == '__main__':
    model_file = sys.argv[1]
    inputs = sys.argv[2]
    test_model(model_file, inputs)

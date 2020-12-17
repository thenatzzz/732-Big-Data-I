import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import random
random.seed()
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('euler code').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

# add more functions as necessary
def cal_interation(each_array):
    num_iteration = 0
    total_sum = 0
    while total_sum< 1:
        total_sum += random.uniform(0,1)
        num_iteration += 1
    return num_iteration

def main(inputs):
    # main logic starts here
    NUM_SAMPLES= int(inputs)
    single_array = sc.parallelize(range(0, NUM_SAMPLES)) 
    #single_array = sc.parallelize(range(0,NUM_SAMPLES),numSlices=100)
    each_iteration = single_array.map(cal_interation)
    total_iterations = each_iteration.sum()
    print("Total number of iterations: ",total_iterations)
    print("Estimated Euler number: ",total_iterations/NUM_SAMPLES)
	
if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)

from pyspark.sql.functions import lower, col
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark

def main():
    sc = pyspark.SparkContext.getOrCreate()
    spark = SparkSession.builder.appName('BigDataApp').getOrCreate()

    # We read from the json file to a Dataframe
    path = "./data/processed/accidents/*.csv"
    dataDF = spark.read.format("csv").option("header", "true").load(path)
    


if __name__ == '__main__':
    main()
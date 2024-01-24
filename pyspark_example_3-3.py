import sys

from pyspark.sql import SparkSession
import pyspark.sql.types as T

# JSON to pyspark dataframe

schema = T.StructType([
    T.StructField("Id", T.IntegerType(), False),
    T.StructField("First", T.StringType(), False),
    T.StructField("Last", T.StringType(), False),
    T.StructField("Url", T.StringType(), False),
    T.StructField("Published", T.StringType(), False),
    T.StructField("Hits", T.IntegerType(), False),
    T.StructField("Campaigns", T.ArrayType(T.StringType(), False), False)
])

# main
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: pyspark_example_3-3.py <file>", file=sys.stderr)
        sys.exit(-1)

    spark = (SparkSession
             .builder
             .appName("Example_3-3")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    blogs_file = sys.argv[1]

    blogs_df = spark.read.json(blogs_file, schema=schema)
    blogs_df.show()

    print(blogs_df.printSchema())
    
    
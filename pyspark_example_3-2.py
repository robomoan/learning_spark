from pyspark.sql import SparkSession
import pyspark.sql.types as T

schema = T.StructType([
    T.StructField("Id", T.IntegerType(), False),
    T.StructField("First", T.StringType(), False),
    T.StructField("Last", T.StringType(), False),
    T.StructField("Url", T.StringType(), False),
    T.StructField("Published", T.StringType(), False),
    T.StructField("Hits", T.IntegerType(), False),
    T.StructField("Campaigns", T.ArrayType(T.StringType(), False), False)
])

data = [[1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter", "LinkedIn"]],
        [2, "Brooke","Wenig","https://tinyurl.2", "5/5/2018", 8908, ["twitter", "LinkedIn"]],
        [3, "Denny", "Lee", "https://tinyurl.3","6/7/2019",7659, ["web", "twitter", "FB", "LinkedIn"]],
        [4, "Tathagata", "Das","https://tinyurl.4", "5/12/2018", 10568, ["twitter", "FB"]],
        [5, "Matei","Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web", "twitter", "FB", "LinkedIn"]],
        [6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568, ["twitter", "LinkedIn"]]
        ]

# main
if __name__ == "__main__":
    spark = (SparkSession
             .builder
             .appName("Example_3-2")
             .getOrCreate())
    spark.sparkContext.setLogLevel('WARN')

    blogs_df = spark.createDataFrame(data, schema)
    blogs_df.show()

    print(blogs_df.printSchema())
    
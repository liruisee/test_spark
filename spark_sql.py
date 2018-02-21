from pyspark.sql import SparkSession

# 初始化sparkSession，注意appName中不能包含空格
spark = SparkSession.builder.appName("PythonSparkSQL").config("spark.some.config.option", "some-value").getOrCreate()

# 将json字符串读入dataframe
df = spark.read.json("D:/spark/examples/src/main/resources/people.json")
df.show()

df.createOrReplaceTempView("people")
sqlDF = spark.sql("SELECT * FROM people")
sqlDF.show()

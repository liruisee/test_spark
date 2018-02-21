from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row

sc = SparkContext()
# 初始化sparkSession，注意appName中不能包含空格
spark = SparkSession.builder.appName("PythonSparkSQL").config("spark.some.config.option", "some-value").getOrCreate()

# 将json字符串读入dataframe
df = spark.read.json("D:/spark/examples/src/main/resources/people.json")
# df.show()

# rdd转化为df
lines = sc.textFile("D:/spark/examples/src/main/resources/people.txt")
parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))
schemaPeople = spark.createDataFrame(people)
schemaPeople.createOrReplaceTempView("people2")
spark.sql("select * from people2 where age<20").show()

# 注册json字符串为临时表，用于执行sql
df.createOrReplaceTempView("people")
spark.sql("SELECT * FROM people").show()

# 注册全局的共享表
df.createGlobalTempView("people")
# 访问全局表的时候，需要加global_temp的前缀
spark.sql("SELECT * FROM global_temp.people").show()


spark.newSession().sql("SELECT * FROM global_temp.people where age is null").show()


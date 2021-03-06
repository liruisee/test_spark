from pyspark import SparkContext


sc = SparkContext('local')
sc.setLogLevel('WARN')

# rdd初始化的几种方式
# data1 = sc.parallelize(("a", 2))
# rdd = sc.textFile("C:/Users/lirui/Desktop/test.txt")
# rdd2 = sc.wholeTextFiles("C:/Users/lirui/Desktop/test.txt")

# #########################transform操作###################################

# rdd算子操作
rdd_test = sc.parallelize([(1, 2), (3, 4), (5, 6), (1, 3), (2, 4), (5, 3)])
rdd_test.cache()
print("测试算子生成完毕", rdd_test.collect())

# map算子
rdd_map = rdd_test.map(lambda row: tuple([2*x for x in row]))
print("map", rdd_map.collect())

# filter算子
rdd_filter = rdd_test.filter(lambda row: 1 in row or 3 in row)
print("filter", rdd_filter.collect())

# flatmap算子（扁平化）
rdd_flatmap = rdd_test.flatMap(lambda x: map(lambda m: 2*m, x))
print("flatmap", rdd_flatmap.collect())

# distinct算子（去重）
rdd_distinct = rdd_flatmap.distinct()
print("distinct", rdd_distinct.collect())

# sample算子，暂时不会用
rdd_sample = rdd_test.sample(False, 0.8, 4)
print("sample", rdd_sample.collect())

# intersection算子（取2个rdd的交集）
rdd_intersection = rdd_test.intersection(rdd_filter)
print("intersection", rdd_intersection.collect())

# union算子（合并2个rdd）
rdd_union = rdd_test.union(rdd_filter)
print("union", rdd_union.collect())

# subtract算子（rdd1排除rdd2的元素）
rdd_subtract = rdd_test.subtract(rdd_filter)
print("subtract", rdd_subtract.collect())

# repartition（增加分区）
rdd_repartition = rdd_test.repartition(5)
rdd_repartition.cache()
print("repartition",rdd_repartition.glom().collect())

# coalesce算子（减少分区）
rdd_coalesce = rdd_repartition.coalesce(3)
print("coalesce", rdd_coalesce.glom().collect())


#keys算子
rdd_keys = rdd_filter.keys()
print("keys", rdd_keys.collect())

# values算子
rdd_values = rdd_filter.values()
print("values", rdd_values.collect())

# mapValues算子（返回键值对，key值不变，value根据传入函数变化）
rdd_mapValues = rdd_test.mapValues(lambda x: 2*x)
print("mapValues", rdd_mapValues.collect())

# leftOutJoin
rdd_leftOutJoin = rdd_test.leftOuterJoin(rdd_filter)
print("leftOutJoin", rdd_leftOutJoin.collect())

# subtractByKey（rdd1的键排除rdd2的键的rdd1的键值对）
rdd_subtractByKey = rdd_test.subtractByKey(rdd_filter)
print("subtractByKey", rdd_subtractByKey.collect())

# groupByKey（只能接受键值对的形式）
rdd_groupByKey = rdd_test.mapValues(lambda x: x).groupByKey()
print("groupBykey", rdd_groupByKey.collect())

# groupBy  不会用
# rdd_groupBy = rdd_test.groupBy()
# print("groupBy", rdd_groupBy.collect())

# reduceByKey（分组执行传入的函数）
rdd_reduceByKey = rdd_test.reduceByKey(lambda x, y: x+y)
print("reduceByKey", rdd_reduceByKey.collect())


# #########################action操作###################################


print("count", rdd_test.count())   # 计数
print("countByKey", rdd_test.countByKey())   # 对每个键计数，返回字典
print("countByValue", rdd_test.countByValue())  # 对每组键值对计数，返回键值对的字典
print("first", rdd_test.first())  # 取rdd的第一个元素
print("top", rdd_test.top(2))  # 取rdd最大的n个元素
print("take", rdd_test.take(2))  # 取rdd前n个元素
print("max", rdd_test.max())  # 取rdd最大的一个元素
print("min", rdd_test.min())  # 取rdd最小的一个元素
print("reduce", rdd_flatmap.reduce(lambda x, y: x+y))  # 适用于一维数组


print("countByKey", rdd_test.mapValues(lambda x: x).countByKey())  # 分组计数，适用于键值对
print("countByValue", rdd_test.mapValues(lambda x: x).countByValue())  # 对键值对分组计数，适用于键值对
print("mean", rdd_test.flatMap(lambda x: map(lambda m: 2*m, x)).mean())  # 求平均值，适用于一维数组
print("sum", rdd_test.flatMap(lambda x: map(lambda m: 2*m, x)).sum())  # 求和，适用于一维数组
print("stdev", rdd_test.flatMap(lambda x: map(lambda m: 2*m, x)).stdev())  # 求标准差，适用于一维数组
print("variance", rdd_test.flatMap(lambda x: map(lambda m: 2*m, x)).variance())  # 求方差，适用于一维数组

sc.stop()


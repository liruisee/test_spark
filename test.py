from pyspark import SparkContext

logFile = "C:/Users/lirui/Desktop/test.txt"
sc = SparkContext('local')
sc.setLogLevel('INFO')
logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print("Lines with a: %i, lines with b: %i"%(numAs, numBs))
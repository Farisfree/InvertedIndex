from pyspark import SparkConf, SparkContext
import os
import re
import pandas as pd
import math

sc = SparkContext.getOrCreate(SparkConf())
data = sc.wholeTextFiles('./demoData')
numFiles = data.count()
wordcount = data.flatMap(lambda x: [((os.path.basename(x[0]).split(".")[0], i), 1) for i in re.split('\\W', x[1])]) \
    .reduceByKey(lambda a, b: a + b)
tf = wordcount.map(lambda x: (x[0][1], (x[0][0], x[1])))
idf = wordcount.map(lambda x: (x[0][1], 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .map(lambda x: (x[0], math.log10(numFiles / x[1])))
tfidf = tf.join(idf) \
    .map(lambda x: (x[1][0][0], (x[0], x[1][0][1] * x[1][1]))) \
    .sortByKey()
df = pd.DataFrame(tfidf.collect(), columns=['File Name', 'TF-IDF'])
df.to_csv('tfidf.csv', index=False)


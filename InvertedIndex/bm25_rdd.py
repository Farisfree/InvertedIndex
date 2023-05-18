from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
import os, re, math, time
from rank_bm25 import BM25Okapi

sc = SparkContext.getOrCreate(SparkConf())

data = sc.wholeTextFiles('./iitfidf')
num = data.count()

avg_length = data.map(lambda x:len(x[1].split(" "))).sum() / num

tmp = data.flatMap(lambda x:[((os.path.basename(x[0]),i),1, len(x[1].split(" "))) for i in re.split('\\W',x[1])])

idf = tmp.map(lambda x:(x[0][1], 1))

# df = wordcount.reduceByKey(lambda a,b:a+b)

wordcount = data.flatMap(lambda x:[((os.path.basename(x[0]),i),1) for i in re.split('\\W',x[1])])
tf = wordcount.reduceByKey(lambda a,b:a+b)


merged_rdd = tmp.map(lambda x: ((x[0][0], x[0][1]), (x[1], x[2]))) \
               .reduceByKey(lambda a, b: (a[0] + b[0], a[1])) \
               .map(lambda x: (x[0][0], x[0][1], x[1][0], x[1][1]))


idf = tf.map(lambda x: (x[0][1], 1)).reduceByKey(lambda a,b:a + b)\
    .map(lambda x: (x[0],math.log((num-x[1]+0.5) / (x[1] + 0.5) + 1)))

result = merged_rdd.map(lambda x:(x[1],x)).join(idf).map(lambda x:x[1][0] + (x[1][1],avg_length,)) # !!!!!

score = result.map(lambda x:(x[0],x[1],x[4]* ( x[2]*(1.2+1) / (x[2] + 1.2*(1-0.75+0.75*x[3]/x[5]) )  )                      ))
#score.collect()
bm25_rdd = score.map(lambda x: (x[1],(x[0],x[2])))
bm25_rdd.collect()




def tokenize(s):
    return re.split("\\W+", s.lower())

def search(query, topN):
    tokens = sc.parallelize(tokenize(query)).map(lambda x: (x, 1) ).collectAsMap()
    bcTokens = sc.broadcast(tokens)

#connect to documents with terms in the Query. to Limit the computation space.
#so that we don't attempt to compute similarity for docs that have no words in common with our query.
    joined_tfidf = nm25_rdd.map(lambda x: (x[0], bcTokens.value.get(x[0], '-'), x[1]) ).filter(lambda x: x[1] != '-' )

#compute the score using aggregateByKey
    # method1:
    '''scount = joined_tfidf.map(lambda a: a[2]).aggregateByKey((0,0),
    (lambda acc, value: (acc[0] + value, acc[1] + 1)),
    (lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])) )'''
    #method2
    scount = joined_tfidf.map(lambda x: (x[2][0], x[2][1])).reduceByKey(lambda a,b:a + b)

    scores = scount.map(lambda x: ( x[1][0]*x[1][1]/len(tokens), x[0]) ).top(topN)

    return scores

search("I love UIC", 5)
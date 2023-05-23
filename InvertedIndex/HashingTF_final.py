from pyspark import SparkConf, SparkContext
import os, re
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, RegexTokenizer, CountVectorizer, IDF
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import NGram, HashingTF
from pyspark.ml import Pipeline


def hashingTFSearch(query):
    sc = SparkContext.getOrCreate(SparkConf())
    spark = SparkSession.builder.appName('MyNgram').getOrCreate()

    data = sc.wholeTextFiles('./iitfidf')

    wordcount = data.map(lambda x: [(os.path.basename(x[0]), x[1].replace("\n", " "))])

    text = wordcount.map(lambda x: (x[0][0], (x[0][1])))
    # 得到一个rrd 内容为(filename, txt内容)
    # wordcount.collect()

    # 创建spark的dataFrame
    documents = spark.createDataFrame(text.collect(), ["filename", "text"])
    # 建立HashingTF 和 IDF 模型
    hasingTF = HashingTF(inputCol="words", outputCol="tf")
    idf = IDF(inputCol="tf", outputCol="tfidf")

    # 建立filename的dataframe 其中包含 filename, text, words, tf,tfidf
    tokenized = Tokenizer(inputCol="text", outputCol="words").transform(documents)
    # tokenized.show()
    tf = hasingTF.transform(tokenized)
    tfidf = idf.fit(tf).transform(tf)
    # tfidf.show()

    # 得到符合条件的数据(dataframe)
    query = "knitting"
    filtered_data = tfidf.filter(tfidf.text.contains(query))

    # 输出结果
    top_documents = filtered_data.select("filename", "tfidf").rdd \
        .map(lambda x: (x[0], x[1].values[0]))

    return top_documents.top(10)

from sre_parse import Tokenizer

from django.http import HttpResponse
from django.shortcuts import render
from pyspark import SparkConf, SparkContext
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.sql import SparkSession, SQLContext
import os, re, math, time




def search(request):
    if request.method == "GET":
        return render(request, "search.html")
    else:
        return render(request, "search.html", {"search_result":"123"})


def pagination(request):
    return render(request, "pagination.html")


def bm25Search(request):
    query = request.POST.get("query")

    result = bm25(query)

    return render(request,"",{"data":result})


def cosineSearch(request):
    query = request.POST.get("query")

    result = cosineSearch(query)

    return render(request,"",{"data":result})


def hashingTFSearch(request):
    query = request.POST.get("query")

    result = hashingTFSearch(query)

    return render(request, "", {"data": result})























def bm25(query):

    spark = SparkSession.builder \
    .master("spark://DPW2-2130026038-FarisGuo:7077") \
    .appName("bm25") \
    .getOrCreate()

    sc = spark.sparkContext

    data = sc.wholeTextFiles('hdfs://DPW2-2130031268-YanweiWu//user/root/demoData')
    num = data.count()

    avg_length = data.map(lambda x: len(x[1].split(" "))).sum() / num

    tmp = data.flatMap(lambda x: [((os.path.basename(x[0]), i), 1, len(x[1].split(" "))) for i in re.split('\\W', x[1])])

    idf = tmp.map(lambda x: (x[0][1], 1))

# df = wordcount.reduceByKey(lambda a,b:a+b)

    wordcount = data.flatMap(lambda x: [((os.path.basename(x[0]), i), 1) for i in re.split('\\W', x[1])])
    tf = wordcount.reduceByKey(lambda a, b: a + b)

    merged_rdd = tmp.map(lambda x: ((x[0][0], x[0][1]), (x[1], x[2]))) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1])) \
    .map(lambda x: (x[0][0], x[0][1], x[1][0], x[1][1]))

    idf = tf.map(lambda x: (x[0][1], 1)).reduceByKey(lambda a, b: a + b) \
    .map(lambda x: (x[0], math.log((num - x[1] + 0.5) / (x[1] + 0.5) + 1)))

    result = merged_rdd.map(lambda x: (x[1], x)).join(idf).map(lambda x: x[1][0] + (x[1][1], avg_length,))  # !!!!!

    score = result.map(lambda x: (x[0], x[1], x[4] * (x[2] * (1.2 + 1) / (x[2] + 1.2 * (1 - 0.75 + 0.75 * x[3] / x[5])))))
# score.collect()
    bm25_rdd = score.map(lambda x: (x[1], (x[0], x[2])))
    tokens = set(query.split(" "))
    result = bm25_rdd.filter(lambda x: x[1][0] in tokens).map(lambda x:(x[0], x[1][1])).reduceByKey(lambda a,b:a+b).top(10)

    return result


def cosineSearch(query):
    sc = SparkContext.getOrCreate(SparkConf())

    spark = SparkSession.builder.appName('MyNgram').getOrCreate()

    data = sc.wholeTextFiles('./iitfidf')
    text = data.flatMap(lambda x: [(os.path.basename(x[0]), x[1].replace("\n", " ").split(" "))])

    text_df = spark.createDataFrame(text.collect(), ["filename", "text"])
    # text.collect() 得到的数据为(filename, [words])

    # 建立HashingTF模型，并对其进行特征分析
    hashingTF = HashingTF(inputCol="text", outputCol="features")
    featurized_data = hashingTF.transform(text_df)
    # 得到的数据结构为 filename, text, features
    # featurized_data.show()

    # 建立IDF模型,得到tfidf值
    idf = IDF(inputCol="features", outputCol="tfidf")
    idf_model = idf.fit(featurized_data)
    tfidf_data = idf_model.transform(featurized_data)
    # tfidf_data.show() 得到的结构内容为(filename, text, feature, tfidf)

    # 输入需要的字段，进行向量解析
    query = "The modern"

    query_test = spark.createDataFrame([(query.split(" "),)], ["text"])
    query_hashing = hashingTF.transform(query_test)
    query_hashing.show()

    # 对得到的query进行特征处理,得到的内容为filename, features, tfidf
    query_tfidf = idf_model.transform(query_hashing)
    query_tfidf.show()

    # 设置query相关的向量设置
    query_vector = query_tfidf.select("text", "features")
    test = query_vector.select("features").collect()[0]["features"]
    test
    norm1 = test.norm(2)

    # 计算余弦相似值，进行排序
    tfidf_rdd = tfidf_data.select("filename", "features").rdd
    tmp = tfidf_rdd.map(lambda x: (x[0], x[1], test.dot(x[1]))) \
        .map(lambda x: (x[0], x[2] / (norm1 * x[1].norm(2))))

    return tmp.top(10)


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
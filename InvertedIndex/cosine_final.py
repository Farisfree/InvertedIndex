from pyspark.ml.feature import HashingTF, IDF
from pyspark.ml.linalg import SparseVector
from pyspark.mllib.linalg.distributed import RowMatrix
from pyspark import SparkConf, SparkContext
import os, re
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, RegexTokenizer, CountVectorizer, IDF
from pyspark.sql.functions import col, udf, expr
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import NGram, HashingTF
from pyspark.ml import Pipeline
from pyspark.ml.linalg import SparseVector
from pyspark.ml.linalg import DenseVector
from pyspark.ml.linalg import Vectors


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


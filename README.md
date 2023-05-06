# InvertedIndex  
这个是我们的Workshop2的小组作业：制作一个类似谷歌的搜索引擎  
 


 
# 要求：  
1. 将数据存储到HDFS上，从HDFS上拉取数据，然后利用Spark进行运算  
2. 分别利用3个Worker和1个Worker做测试，记录下区别  
3. 用Django作为主要框架  
4. 尽量比较美观的实现前端界面
5. 使用一些比较高级的算法进行Map & Reduce计算
6. 利用爬虫获取至少1024MB的纯英文TXT文本数据  
  
  
# Tips
1. 你可以在开始时用较小的数据集进行测试，然后用较大的数据集演示你的系统的性能。  
    请详细描述你用来收集报告中的数据的方法。考虑一下加快数据收集过程的方法。  
2. 使用Hadoop/Spark MapReduce编写一个程序来生成倒置的索引。(25分)  
3. Rank是决定一个特定的文件出现在搜索引擎查询结果中的位置。
    有许多不同的Rank算法:
      https://en.wikipedia.org/wiki/Tf%E2%80%93idf  
      https://zhuanlan.zhihu.com/p/31197209   
      https://en.wikipedia.org/wiki/PageRank  
  如果你在大量的文本文件上搜索，你可以实现TF-IDF排名算法，根据你的关键词所在的txt文件的相关性，对你的查询结果进行排名。使用Hadoop/Spark MapReduce编写一个程序来生成倒置的索引。(25分)  
  
# 需要提交的内容  
a)	Codes with proper comments for each step above.  
b)	A written report in PDF format with name “GROUP_NO_GROUP_NAME.docx” recording what you did, how you do it and what result you get in each step.  
c)	Prepare a presentation of ~8 minutes each group, clearly list the contribution of each member.  
d)	Zip your code, report and presentation as GROUP_NO_GROUP_NAME.zip and submit to iSpace.  

# 可参考数据集：  
1.	Project Gutenberg, a free e-book database: http://www.gutenberg.org/ebooks/
2.	GitHub collection of datasets: https://github.com/awesomedata/awesome-public-datasets
3.	Kaggle collection of datasets: https://www.kaggle.com/datasets
4.	Other methods you can come up with





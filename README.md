# spark-learn-samples
一些自己以前写的用来学习测试的spark例子持续更新
### 已经更新：


#####1.spark2.0 数据读取DataSource的扩展 主要继承BaseRelation
 
#####2.spark2.0 数据转换器 Transfomer的扩展

#####3.spark2.0 数据评估器算法 Estimater的扩展


#####A.转换器的扩展  一般对数据集进行转换 可以用来特征工程的转换。

实现方法:直接实现通用的Transformer
这种方式主要实现2个方法 

第一个transform 对 dataframe 进行转换  
第二个 transformSchema 对dataframe的schema信息进行转换 
实现方法B.如果只是基本的原子性转换 可以直接继承 UnaryTransformer
#####B.评估器的扩展  对数据集进行转换 训练 预测 所以可以用来实现算法。 

A.直接实现Estimater 

B.直接继承Predictor 








package cn.itcast.tags.ml.features

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructType}

/*
 * 读取鸢尾花数据集，封装特征至 features
 *
 * 基于 DataFrame Api 来实现
 */
object IrisFeaturesDemo {

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
            .master("local[*]")
            .appName(this.getClass.getSimpleName.stripPrefix("$"))
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()

        import spark.implicits._

        // 1 加载鸢尾花数据集
        val irisSchema: StructType = new StructType()
            .add("sepal_length", DoubleType, nullable = true)
            .add("sepal_width", DoubleType, nullable = true)
            .add("petal_length", DoubleType, nullable = true)
            .add("petal_width", DoubleType, nullable = true)
            .add("class", StringType, nullable = true)

        val rawIrisDF = spark.read
            .option("sep", ",")
            .option("header", "false")
            .option("inferSchema", "false")
            .schema(irisSchema)
            .csv("datas/iris/iris.data")

        rawIrisDF.show(10, truncate = false)

        // 2 将特征（萼片长宽，花瓣长宽）封装到向量中
        // 使用 VectorAssembler 工具类 https://spark.apache.org/docs/latest/ml-features.html#vectorassembler
        val assembler = new VectorAssembler()
            // .setInputCols(Array("sepal_length", "sepal_width", "petal_length", "petal_width"))
            .setInputCols(rawIrisDF.columns.dropRight(1))
            .setOutputCol("features")

        val featuresDF = assembler.transform(rawIrisDF)

        /*
         * root
         * |-- sepal_length: double (nullable = true)
         * |-- sepal_width: double (nullable = true)
         * |-- petal_length: double (nullable = true)
         * |-- petal_width: double (nullable = true)
         * |-- class: string (nullable = true)
         * |-- features: vector (nullable = true)
         */
        featuresDF.printSchema()
        featuresDF.show(10, truncate = false)

        // 3 将标签转换为数值类型
        val indexer: StringIndexer = new StringIndexer()
            .setInputCol("class")
            .setOutputCol("label")

        val indexed = indexer.fit(featuresDF).transform(featuresDF)

        /*
         * +------------+-----------+------------+-----------+-----------+-----------------+-----+
         * |sepal_length|sepal_width|petal_length|petal_width|class      |features         |label|
         * +------------+-----------+------------+-----------+-----------+-----------------+-----+
         * |5.1         |3.5        |1.4         |0.2        |Iris-setosa|[5.1,3.5,1.4,0.2]|0.0  |
         * |4.9         |3.0        |1.4         |0.2        |Iris-setosa|[4.9,3.0,1.4,0.2]|0.0  |
         * |4.6         |3.4        |1.4         |0.3        |Iris-setosa|[4.6,3.4,1.4,0.3]|0.0  |
         * |5.0         |3.4        |1.5         |0.2        |Iris-setosa|[5.0,3.4,1.5,0.2]|0.0  |
         * |4.4         |2.9        |1.4         |0.2        |Iris-setosa|[4.4,2.9,1.4,0.2]|0.0  |
         * |4.9         |3.1        |1.5         |0.1        |Iris-setosa|[4.9,3.1,1.5,0.1]|0.0  |
         * +------------+-----------+------------+-----------+-----------+-----------------+-----+
         */
        indexed.printSchema()
        indexed.show(10, truncate = false)

        /*
         * 机器学习核心三要素：数据 + 算法 = 模型
         * - 调优过程中，最重要的就是特征数据 Features，如果特征数据比较好就可以得到更好的模型
         * - 所以在实际的开发中特征数据需要各种转换操作，比如正则化、归一化、标准化等等
         * - 原因在于不同维度的特征值范围跨度不一样（每一列的数据范围跨度），导致模型不准确，所以需要进行转换操作
         */

        // 1 将特征数据进行标准化转换
        // https://spark.apache.org/docs/latest/ml-features.html#standardscaler
        val scaler = new StandardScaler()
            .setInputCol("features")
            .setOutputCol("scaledFeatures")
            // 是否使用标准差缩放
            .setWithStd(true)
            // 是否使用平均值缩放
            .setWithMean(false)

        val scalerModel = scaler.fit(indexed).transform(indexed)

        /*
         * +-----------------+-----+-------------------------------------------------------------------------+
         * |features         |label|scaledFeatures                                                           |
         * +-----------------+-----+-------------------------------------------------------------------------+
         * |[5.1,3.5,1.4,0.2]|0.0  |[6.158928408838787,8.072061621390857,0.7934616853039358,0.26206798787142]|
         * |[4.9,3.0,1.4,0.2]|0.0  |[5.9174018045706,6.9189099611921625,0.7934616853039358,0.26206798787142] |
         * |[4.7,3.2,1.3,0.2]|0.0  |[5.675875200302412,7.38017062527164,0.7367858506393691,0.26206798787142] |
         * |[4.6,3.1,1.5,0.2]|0.0  |[5.555111898168318,7.149540293231902,0.8501375199685027,0.26206798787142]|
         * |[5.0,3.6,1.4,0.2]|0.0  |[6.038165106704694,8.302691953430596,0.7934616853039358,0.26206798787142]|
         * |[5.4,3.9,1.7,0.4]|0.0  |[6.52121831524107,8.99458294954981,0.9634891892976364,0.52413597574284]  |
         * |[4.6,3.4,1.4,0.3]|0.0  |[5.555111898168318,7.841431289351117,0.7934616853039358,0.39310198180713]|
         * |[5.0,3.4,1.5,0.2]|0.0  |[6.038165106704694,7.841431289351117,0.8501375199685027,0.26206798787142]|
         * |[4.4,2.9,1.4,0.2]|0.0  |[5.313585293900131,6.688279629152423,0.7934616853039358,0.26206798787142]|
         * |[4.9,3.1,1.5,0.1]|0.0  |[5.9174018045706,7.149540293231902,0.8501375199685027,0.13103399393571]  |
         * +-----------------+-----+-------------------------------------------------------------------------+
         */
        scalerModel.show(10, truncate = false)

        /*
         * 选择一个分类算法构建特征模型
         * - 决策树分类算法（Decision Tree）
         * - 朴素贝叶斯分类算法（Naive Bayes），适合于构建文本数据特征分类，比如垃圾邮件情感分析
         * - 逻辑回归分类算法（Logistics Regression）
         * - 线性支持向量机分类算法（Linear SVM）
         * + 神经网络相关的分类算法，比如多层感知机
         * + 集成融合算法：随机森林分类算法（RF），梯度提升树算法（GBT）
         */

        /*
         * 使用逻辑回归分类算法
         */
        val lr: LogisticRegression = new LogisticRegression()
            // 设置特征值的列名称和标签列名称
            .setFeaturesCol("scaledFeatures")
            .setLabelCol("label")
            // 设置算法的超参数，迭代次数
            .setMaxIter(100)
            // 设置属于二分类还是多分类
            .setFamily("multinomial")
            // 是否对特征数据进行标准化
            .setStandardization(true)
            // 设置弹性化参数
            .setRegParam(0)

        // 将数据应用到算法中
        val lrModel: LogisticRegressionModel = lr.fit(scalerModel)

        /*
         * 评估得到的模型
         */
        // 0.9733333333333334
        println("模型准确度" + lrModel.summary.accuracy)
        // Array(1.0, 0.96, 0.96)
        println("模型精确度" + lrModel.summary.precisionByLabel.mkString("Array(", ", ", ")"))
        // -5.798835106190835  13.323005447127198  -14.086482300220093   -13.763604967380042
        // 5.316979206232901   -4.856481059376392  0.019341070899700507  0.019748360791430865
        // 0.4818558999579335  -8.466524387750807  14.067141229320393    13.743856606588611
        println("混淆矩阵：\n" + lrModel.coefficientMatrix)

        spark.stop()
    }

}

package cn.itcast.tags.ml.classification

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/*
 * 使用逻辑回归，基于泰坦尼克号数据集构建分类模型
 */
object TitanicLrClassification {

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
            .master("local[*]")
            .appName(this.getClass.getSimpleName.stripPrefix("$"))
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()

        import spark.implicits._

        /*
         * 1 读取数据集
         */
        val rawTitanicDF = spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv("datas/titanic/train.csv")

        rawTitanicDF.cache()
        rawTitanicDF.printSchema()
        rawTitanicDF.show(3, truncate = false)

        /*
         * 2 数据准备：选取特征值，进行特征工程
         * - Age 年龄缺省值：使用平均年龄代替
         * - Sex 类别特征：使用独热码处理
         * - Features 特征组合：使用 VectorAssembler 进行组合
         */

        // 2.1 年龄处理
        val avgAge: Double = rawTitanicDF
            .select($"Age")
            .filter($"Age".isNotNull)
            .select(round(avg($"Age"), 2).as("avgAge"))
            .first()
            .getAs[Double](0)

        println("avgAge: " + avgAge)

        val ageTitanicDF: DataFrame = rawTitanicDF.select(
            $"Survived".as("label"),
            $"Pclass", $"Sex", $"SibSp", $"Parch", $"Fare", $"Age",
            // 当年龄为 null 时使用平均年龄代替
            when($"Age".isNotNull, $"Age").otherwise(avgAge).as("defaultAge")
        )

        /*
         * +-----+------+------+-----+-----+-------+----+----------+
         * |label|Pclass|Sex   |SibSp|Parch|Fare   |Age |defaultAge|
         * +-----+------+------+-----+-----+-------+----+----------+
         * |0    |3     |male  |1    |0    |7.25   |22.0|22.0      |
         * |1    |1     |female|1    |0    |71.2833|38.0|38.0      |
         * |1    |3     |female|0    |0    |7.925  |26.0|26.0      |
         * +-----+------+------+-----+-----+-------+----+----------+
         */
        ageTitanicDF.show(3, truncate = false)

        // 2 对 Sex 字段类型特征转换 male -> [1.0, 0.0] female [0.0, 1.0]
        val indexer = new StringIndexer()
            .setInputCol("Sex")
            .setOutputCol("sexIndex")
        val indexerTitanicDF: DataFrame = indexer.fit(ageTitanicDF).transform(ageTitanicDF)

        val oneHotEncoder = new OneHotEncoder()
            .setInputCol("sexIndex")
            .setOutputCol("sexVector")
            .setDropLast(false)
        val sexTitanicDF: DataFrame = oneHotEncoder.fit(indexerTitanicDF).transform(indexerTitanicDF)

        // 3 特征组合
        val vectorAssembler = new VectorAssembler()
            .setInputCols(Array("Pclass", "sexVector", "SibSp", "Parch", "Fare", "defaultAge"))
            .setOutputCol("features")

        val titanicDF = vectorAssembler.transform(sexTitanicDF)

        /*
         * sexVector - 稀疏向量表示
         * +-----+------+------+-----+-----+-------+----+----------+--------+-------------+----------------------------------+
         * |label|Pclass|Sex   |SibSp|Parch|Fare   |Age |defaultAge|sexIndex|sexVector    |features                          |
         * +-----+------+------+-----+-----+-------+----+----------+--------+-------------+----------------------------------+
         * |0    |3     |male  |1    |0    |7.25   |22.0|22.0      |0.0     |(2,[0],[1.0])|[3.0,1.0,0.0,1.0,0.0,7.25,22.0]   |
         * |1    |1     |female|1    |0    |71.2833|38.0|38.0      |1.0     |(2,[1],[1.0])|[1.0,0.0,1.0,1.0,0.0,71.2833,38.0]|
         * |1    |3     |female|0    |0    |7.925  |26.0|26.0      |1.0     |(2,[1],[1.0])|[3.0,0.0,1.0,0.0,0.0,7.925,26.0]  |
         * +-----+------+------+-----+-----+-------+----+----------+--------+-------------+----------------------------------+
         */
        titanicDF.show(3, truncate = false)

        /*
         * 3 构建模型
         */

        // 3.1 划分训练集和测试集
        val Array(trainingDF, testingDF) = titanicDF.randomSplit(Array(0.8, 0.2))
        trainingDF.cache()

        // 3.2 使用算法和数据构建模型
        val logisticRegression: LogisticRegression = new LogisticRegression()
            .setLabelCol("label")
            .setFeaturesCol("features")
            .setPredictionCol("prediction")
            // 二分类
            .setFamily("binomial")
            .setStandardization(true)
            // 超参数
            .setMaxIter(100)
            .setRegParam(0.1)
            .setElasticNetParam(0.8)

        val lrModel: LogisticRegressionModel = logisticRegression.fit(trainingDF)

        // 3.3 模型评估
        // 打印模型系数
        println("coefficientMatrix: " + lrModel.coefficientMatrix)
        println("interceptVector: " + lrModel.interceptVector)

        // 模型准确度
        println("accuracy: " + lrModel.summary.accuracy)

        // 使用测试集进行模型预测
        val predictedDF: DataFrame = lrModel.transform(testingDF)
        predictedDF.select("label", "prediction", "probability", "features").show(50, false)

        // 多分类评估器
        val accuracy = new MulticlassClassificationEvaluator()
            .setLabelCol("label")
            .setPredictionCol("prediction")
            // 指标："f1", "weightedPrecision", "weightedRecall", "accuracy" 等
            .setMetricName("accuracy")
            .evaluate(predictedDF)
        println("accuracy: " + accuracy)

        spark.close()
    }

}

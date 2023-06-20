package cn.itcast.tags.ml.classification

import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel, VectorIndexer, VectorIndexerModel}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
 * 使用管道
 */
object PipelineClassification {
    
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .appName(this.getClass.getSimpleName.stripSuffix("$"))
            .master("local[4]")
            .getOrCreate()
        import spark.implicits._
        
        // 1. 加载数据
        val dataframe: DataFrame = spark.read
            .format("libsvm")
            .load("datas/mllib/sample_libsvm_data.txt")
        //dataframe.printSchema()
        //dataframe.show(10, truncate = false)
        
        // 2. 针对原始数据集划分数据集：训练数据和测试数据
        val Array(trainingDF, testingDF) = dataframe.randomSplit(Array(0.8, 0.2))
        
        // 3. 特征工程：特征提取、特征转换及特征选择
        // a. 将标签值label，转换为索引，从0开始，到 K-1
        val labelIndexer: StringIndexerModel = new StringIndexer()
            .setInputCol("label")
            .setOutputCol("index_label")
            .fit(dataframe)
        
        // b. 对类别特征数据进行特殊处理, 当每列的值的个数小于设置K，那么此列数据被当做类别特征，自动进行索引转换
        val featureIndexer: VectorIndexerModel = new VectorIndexer()
            .setInputCol("features")
            .setOutputCol("index_features")
            .setMaxCategories(4)
            .fit(dataframe)
        
        // 4. 使用决策树算法构建分类模型
        val dtc: DecisionTreeClassifier = new DecisionTreeClassifier()
            .setLabelCol("index_label")
            .setFeaturesCol("index_features")
            // 超参数：设置决策树算法相关超参数
            .setMaxDepth(5)
            // 超参数：此值必须大于等于类别特征类别个数
            .setMaxBins(32)
            // 超参数：也可以是香农熵：entropy
            .setImpurity("gini")
        
        // 5 构建管道，设置 Stage，每个 Stage 要么是算法（模型学习器）要么是模型（转换器）
        val pipeline: Pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, dtc))
        
        // 6 应用数据集，训练管道模型
        val pipelineModel: PipelineModel = pipeline.fit(trainingDF)
        
        // 获取决策树模型
        val dtcModel: DecisionTreeClassificationModel = pipelineModel.stages(2).asInstanceOf[DecisionTreeClassificationModel]
        println(dtcModel.toDebugString)
        
        // 7. 模型评估，计算准确度
        val predictionDF: DataFrame = pipelineModel.transform(testingDF)
        predictionDF.printSchema()
        
        predictionDF.select($"label", $"index_label", $"probability", $"prediction").show(20, truncate = false)
        
        // 多分类评估器
        val evaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("index_label")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")
        val accuracy: Double = evaluator.evaluate(predictionDF)
        println(s"Accuracy = $accuracy")
        
        spark.stop()
    }
}

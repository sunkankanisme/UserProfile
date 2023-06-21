package cn.itcast.tags.tools

import cn.itcast.tags.config.ModelConfig
import cn.itcast.tags.utils.HdfsUtils
import org.apache.spark.ml.{Model, Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.{ClusteringEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.DataFrame

/*
 * 算法模型工具类：专门依据数据集训练算法模型，保存及加载
 */
object MLModelTools {
    
    /**
      * 加载模型，如果模型不存在，使用算法训练模型
      *
      * @param dataframe 训练数据集
      * @param mlType    算法名称
      *
      * @return Model 模型
      */
    def loadModel(dataframe: DataFrame, mlType: String, modelClass: Class[_]): Model[_] = {
        val modelPath: String = s"${ModelConfig.MODEL_BASE_PATH}/${modelClass.getSimpleName.stripSuffix("$")}"
        
        // 1. 判断模型是否存在，存在直接加载
        val conf = dataframe.sparkSession.sparkContext.hadoopConfiguration
        if (HdfsUtils.exists(conf, modelPath)) {
            println(s"正在从【$modelPath】加载模型 ...")
            mlType.toLowerCase match {
                case "rfm" => KMeansModel.load(modelPath)
                case "rfe" => KMeansModel.load(modelPath)
                case "psm" => KMeansModel.load(modelPath)
                case "usg" => PipelineModel.load(modelPath)
            }
        } else {
            // 2. 如果模型不存在训练模型，获取最佳模型及保存模型
            println(s"正在训练模型 ...")
            val bestModel = mlType.toLowerCase match {
                case "rfm" => trainBestKMeansModel(dataframe, 5)
                case "rfe" => trainBestKMeansModel(dataframe, 4)
                case "psm" => trainBestKMeansModel(dataframe, 5)
                case "usg" => trainBestPipelineModel(dataframe)
            }
            
            // 保存模型
            println(s"保存最佳模型 ...")
            bestModel.save(modelPath)
            
            // 返回模型
            bestModel
        }
    }
    
    /**
      * 调整算法超参数，获取最佳模型
      *
      * @param dataframe 数据集
      */
    def trainBestKMeansModel(dataframe: DataFrame, kClusters: Int): KMeansModel = {
        // 1.设置超参数的值
        val maxIters = Array(5, 10, 20, 50)
        
        // 2.不同超参数的值，训练模型
        val models: Array[(Double, KMeansModel, Int)] = maxIters.map {
            maxIter => {
                // a. 使用KMeans算法应用数据训练模式
                val kMeans: KMeans = new KMeans()
                    .setFeaturesCol("features")
                    .setPredictionCol("prediction")
                    .setK(kClusters)
                    .setMaxIter(maxIter)
                    .setSeed(31)
                
                // b. 训练模式
                val model: KMeansModel = kMeans.fit(dataframe)
                
                // c. 模型评估指标 - 轮廓系数
                val predictions = model.transform(dataframe)
                val silhouette: Double = new ClusteringEvaluator().evaluate(predictions)
                
                // d. 返回三元组(评估指标, 模型, 超参数的值)
                (silhouette, model, maxIter)
            }
        }
        
        models.foreach(println)
        
        // 3.获取最佳模型
        val (_, bestModel, _) = models.maxBy(tuple => tuple._1)
        
        // 4.返回最佳模型
        bestModel
    }
    
    /**
      * 采用K-Fold交叉验证方式，调整超参数获取最佳PipelineModel模型
      *
      * @param dataframe 数据集
      */
    def trainBestPipelineModel(dataframe: DataFrame): PipelineModel = {
        // a. 特征向量化
        val assembler: VectorAssembler = new VectorAssembler()
            .setInputCols(Array("color", "product"))
            .setOutputCol("raw_features")
        
        // b. 类别特征进行索引
        val indexer: VectorIndexer = new VectorIndexer()
            .setInputCol("raw_features")
            .setOutputCol("features")
            .setMaxCategories(30)
        
        // .fit(dataframe)
        // c. 构建决策树分类器
        val dtc: DecisionTreeClassifier = new DecisionTreeClassifier()
            .setFeaturesCol("features")
            .setLabelCol("label")
            .setPredictionCol("prediction")
        
        // d. 构建Pipeline管道流实例对象
        val pipeline: Pipeline = new Pipeline().setStages(
            Array(assembler, indexer, dtc)
        )
        
        // e. 构建参数网格，设置超参数的值
        val paramGrid: Array[ParamMap] = new ParamGridBuilder()
            .addGrid(dtc.maxDepth, Array(5, 10))
            .addGrid(dtc.impurity, Array("gini", "entropy"))
            .addGrid(dtc.maxBins, Array(32, 64))
            .build()
        
        // f. 多分类评估器
        val evaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("label")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")
        
        // is areaUnderROC.
        val cv = new CrossValidator()
            // 设置算法
            .setEstimator(pipeline)
            // 设置评估器
            .setEvaluator(evaluator)
            // 设置超参数网格
            .setEstimatorParamMaps(paramGrid)
            // 将数据集划分为几份，一份为测试集，K-1 为训练集
            .setNumFolds(3)
            .setParallelism(2)
        
        val cvModel = cv.fit(dataframe)
        val bestModel = cvModel.bestModel.asInstanceOf[PipelineModel]
        
        bestModel
    }
}

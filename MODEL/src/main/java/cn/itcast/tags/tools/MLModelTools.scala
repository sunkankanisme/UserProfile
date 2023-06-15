package cn.itcast.tags.tools

import cn.itcast.tags.utils.HdfsUtils
import org.apache.spark.ml.Model
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
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
    def loadModel(dataframe: DataFrame, mlType: String, modelPath: String): Model[_] = {
        // 1. 判断模型是否存在，存在直接加载
        val conf = dataframe.sparkSession.sparkContext.hadoopConfiguration
        if (HdfsUtils.exists(conf, modelPath)) {
            println(s"正在从【$modelPath】加载模型 ...")
            mlType.toLowerCase match {
                case "kmeans" => KMeansModel.load(modelPath)
                case "dtc" => DecisionTreeClassificationModel.load(modelPath)
            }
        } else {
            // 2. 如果模型不存在训练模型，获取最佳模型及保存模型
            println(s"正在训练模型 ...")
            val bestModel = mlType.toLowerCase match {
                case "kmeans" => trainBestKMeansModel(dataframe)
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
    def trainBestKMeansModel(dataframe: DataFrame): KMeansModel = {
        // 1.设置超参数的值
        val maxIters = Array(5, 10, 20, 50)
        
        // 2.不同超参数的值，训练模型
        val models: Array[(Double, KMeansModel, Int)] = maxIters.map {
            maxIter => {
                // a. 使用KMeans算法应用数据训练模式
                val kMeans: KMeans = new KMeans()
                    .setFeaturesCol("features")
                    .setPredictionCol("prediction")
                    .setK(4)
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
}

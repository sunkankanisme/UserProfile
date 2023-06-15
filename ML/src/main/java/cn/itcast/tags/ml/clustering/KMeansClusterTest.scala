package cn.itcast.tags.ml.clustering

import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 使用KMeans算法对单值数据进行聚类操作
  */
object KMeansClusterTest {
    
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .appName(this.getClass.getSimpleName.stripSuffix("$"))
            .master("local[2]")
            .getOrCreate()
        import spark.implicits._
        
        // 1. 模拟数据集
        val datas: DataFrame = Seq(
            1.0, 1.1, 1.2, 1.01, 1.21,
            2.0, 1.94, 2.1, 1.89, 2.21,
            2.89, 3.1, 3.08, 3.01, 2.98
        ).toDF("point")
        
        // 2. 组合特征
        val dataframe: DataFrame = new VectorAssembler()
            .setInputCols(Array("point"))
            .setOutputCol("features")
            .transform(datas)
        
        // 3. 构建KMeans算法
        val kmeans: KMeans = new KMeans()
            // 设置输入特征列名称和输出列的名名称
            .setFeaturesCol("features")
            .setPredictionCol("prediction")
            // 设置K值为3
            .setK(3)
            // 设置最大的迭代次数
            .setMaxIter(80)
        
        // 4. 应用数据集训练模型
        val kMeansModel: KMeansModel = kmeans.fit(dataframe)
        
        // 获取聚类的簇中心点
        // [2.028]
        // [3.0120000000000005]
        // [1.1039999999999999]
        kMeansModel.clusterCenters.foreach(println)
        
        // 5. 使用轮廓系数进行模型评估
        val predictionDF: DataFrame = kMeansModel.transform(dataframe)
        val silhouette = new ClusteringEvaluator()
            .setMetricName("silhouette")
            .evaluate(predictionDF)
        // 0.9709776419195312
        println("聚类模型轮廓系数：" + silhouette)
        
        // 6. 使用模型预测
        predictionDF.show(20, truncate = false)
        predictionDF
            .groupBy($"prediction")
            .count()
            .show(20, truncate = false)
        
        // 应用结束，关闭资源
        spark.stop()
    }
    
}

package cn.itcast.tags.ml.clustering

import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.mllib.clustering.DistanceMeasure
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable

/**
 * 针对鸢尾花数据集进行聚类，使用KMeans算法
 * - 采用肘部法则 Elbow 获取 K 的值
 * - 使用轮廓系数进行评估
 */
object IrisElbowClustering {

    def main(args: Array[String]): Unit = {
        // 构建SparkSession实例对象
        val spark: SparkSession = SparkSession.builder()
            .appName(this.getClass.getSimpleName.stripSuffix("$"))
            .master("local[4]")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()

        import spark.implicits._

        // 1. 加载鸢尾花数据，使用libsvm格式
        val irisDF: DataFrame = spark.read
            .format("libsvm")
            .load("datas/iris_kmeans.txt")
        irisDF.cache()

        /*
         * 2. 设置不同K，从2开始到6，采用肘部法确定K值
         *
         * setDefault(
         * k -> 2,
         * maxIter -> 20,
         * initMode -> MLlibKMeans.K_MEANS_PARALLEL,
         * initSteps -> 2,
         * tol -> 1e-4,
         * distanceMeasure -> DistanceMeasure.EUCLIDEAN
         * )
         */
        val clusters: immutable.Seq[(Int, Double, String)] = (2 to 6).map {
            k => {
                // a. 构建KMeans算法实例
                val kmeans: KMeans = new KMeans()
                    .setFeaturesCol("features")
                    .setPredictionCol("prediction")
                    // 设置K
                    .setK(k)
                    .setMaxIter(20)
                    // 设置距离计算方式：欧式距离和余弦距离
                    //.setDistanceMeasure(DistanceMeasure.EUCLIDEAN)
                    .setDistanceMeasure(DistanceMeasure.COSINE)

                // b. 算法应用数据训练模型
                val kmeansModel: KMeansModel = kmeans.fit(irisDF)

                // c. 模型预测，对数据聚类
                val predictionDF: DataFrame = kmeansModel.transform(irisDF)

                // 统计出各个类簇中的个数
                val preResult: String = predictionDF
                    .groupBy($"prediction")
                    .count()
                    .select($"prediction", $"count")
                    .as[(Int, Long)]
                    .rdd
                    .collect()
                    .mkString("[", ",", "]")

                // d. 模型评估器
                val evaluator: ClusteringEvaluator = new ClusteringEvaluator()
                    .setFeaturesCol("features")
                    .setPredictionCol("prediction")
                    // 距离计算方式欧式距离
                    .setDistanceMeasure("squaredEuclidean")
                    // 距离计算方式余弦距离
                    // .setDistanceMeasure("cosine")
                    // 评估指标轮廓系数
                    .setMetricName("silhouette")

                // e. 计算轮廓系数
                val silhouette: Double = evaluator.evaluate(predictionDF)

                // f. 返回元组
                (k, silhouette, preResult)
            }
        }

        /*
         * 打印聚类中K值及SH 轮廓系数
         * (2, 0.8462156076702896,  [(1,50),(0,100)])
         * (3, 0.6295595234430768,  [(2,45),(1,50),(0,55)])
         * (4, 0.4724308092411572,  [(2,41),(0,50),(1,31),(3,28)])
         * (5, 0.33214140349010296, [(2,23),(4,23),(0,50),(3,22),(1,32)])
         * (6, 0.1036611740266998,  [(4,22),(5,16),(2,27),(0,28),(3,28),(1,29)])
         *
         * 从上述结果可知，当 K=3 时，聚类是比较好的
         * 使用余弦距离计算，结果如下，同样表明 K=3 时，聚类效果最好
         *
         * (2, 0.9579554849242663, [(1,50),(0,100)])
         * (3, 0.748993658639686,  [(2,45),(1,50),(0,55)])
         * (4, 0.6605645282592109, [(2,41),(0,50),(1,31),(3,28)])
         * (5, 0.5535448834740326, [(2,23),(4,23),(0,50),(3,22),(1,32)])
         * (6, 0.3999130401269345, [(4,22),(5,16),(2,27),(0,28),(3,28),(1,29)])
         */
        clusters.foreach(println)

        // 应用结束，关闭资源
        spark.stop()
    }

}

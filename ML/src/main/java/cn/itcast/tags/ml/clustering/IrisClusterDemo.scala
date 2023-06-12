package cn.itcast.tags.ml.clustering

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.SparkSession

/*
 * 使用鸢尾花数据集，基于 KMeans 算法构建模型
 */
object IrisClusterDemo {

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
            .master("local[*]")
            .appName(this.getClass.getSimpleName.stripPrefix("$"))
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()

        import spark.implicits._

        /*
         * 1 加载数据源
         */
        val irisDF = spark.read.format("libsvm").load("datas/iris_kmeans.txt")

        /*
         * +-----+-------------------------------+
         * |label|features                       |
         * +-----+-------------------------------+
         * |1.0  |(4,[0,1,2,3],[5.1,3.5,1.4,0.2])|
         * |1.0  |(4,[0,1,2,3],[4.9,3.0,1.4,0.2])|
         * |1.0  |(4,[0,1,2,3],[4.7,3.2,1.3,0.2])|
         * +-----+-------------------------------+
         */
        irisDF.show(3, truncate = false)

        /*
         * 2 构建算法
         */
        val kmeans = new KMeans().setK(3).setMaxIter(20).setSeed(1L)
        val model = kmeans.fit(irisDF)

        // 打印中心点
        model.clusterCenters.foreach(println)

        /*
         * 3 模型评估
         */
        val predictions = model.transform(irisDF)
        val evaluator = new ClusteringEvaluator()
        val silhouette = evaluator.evaluate(predictions)
        println(s"Silhouette with squared euclidean distance = $silhouette")

        spark.stop()
    }

}

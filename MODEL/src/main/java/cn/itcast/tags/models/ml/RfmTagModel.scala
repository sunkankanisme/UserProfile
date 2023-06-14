package cn.itcast.tags.models.ml

import cn.itcast.tags.config.ModelConfig
import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import cn.itcast.tags.utils.HdfsUtils
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Column, DataFrame}

/*
 * 挖掘类型模型开发，客户价值 RFM
 */
class RfmTagModel extends AbstractModel("客户价值RFM", ModelType.ML) {

    override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
        import businessDF.sparkSession.implicits._

        /*
         * +--------+--------------------+-----------+----------+
         * |memberid|ordersn             |orderamount|finishtime|
         * +--------+--------------------+-----------+----------+
         * |265     |gome_792756751164275|2479.45    |1659715200|
         * |208     |jd_14090106121770839|2449.00    |1662566400|
         * |673     |jd_14090112394810659|1099.42    |1664640000|
         * +--------+--------------------+-----------+----------+
         */
        // businessDF.show(3, truncate = false)

        /*
         * +---+--------+----+-----+
         * |id |name    |rule|level|
         * +---+--------+----+-----+
         * |362|高价值  |0   |5    |
         * |363|中上价值|1   |5    |
         * |364|中价值  |2   |5    |
         * |365|中下价值|3   |5    |
         * |366|超低价值|4   |5    |
         * +---+--------+----+-----+
         */
        // tagDF.filter("level == 5").show(10, truncate = false)

        /*
         * 1 计算 RFM
         */
        val rfmDF: DataFrame = businessDF.groupBy($"memberid")
            .agg(
                max($"finishtime").as("max_finishtime"),
                count($"ordersn").as("frequency"),
                sum($"orderamount".cast(DataTypes.createDecimalType(10, 2))).as("monetary")
            )
            .select(
                $"memberid".as("userId"),
                datediff(current_timestamp(), from_unixtime($"max_finishtime")).as("recency"),
                $"frequency",
                $"monetary"
            )

        /*
         * +------+-------+---------+---------+
         * |userId|recency|frequency|monetary |
         * +------+-------+---------+---------+
         * |1     |11     |151      |259893.81|
         * |102   |11     |115      |248682.97|
         * |107   |14     |141      |246028.82|
         * +------+-------+---------+---------+
         */
        // rfmDF.show(3, truncate = false)

        /*
         * 2 按照 RFM 规则进行打分
         */
        val rColumn: Column = when(col("recency").between(1, 3), 5.0)
            .when($"recency".between(4, 6), 4.0)
            .when($"recency".between(7, 9), 3.0)
            .when($"recency".between(10, 15), 2.0)
            .otherwise(1.0)
            .as("r_score")

        val fColumn: Column = when(col("frequency").between(1, 49), 1.0)
            .when(col("frequency").between(50, 99), 2.0)
            .when(col("frequency").between(100, 149), 3.0)
            .when(col("frequency").between(150, 199), 4.0)
            .when(col("frequency").geq(200), 5.0)
            .as("f_score")

        val mColumn: Column = when(col("monetary").lt(10000), 1.0)
            .when(col("monetary").between(10000, 49999), 2.0)
            .when(col("monetary").between(50000, 99999), 3.0)
            .when(col("monetary").between(100000, 199999), 4.0)
            .when(col("monetary").geq(200000), 5.0)
            .as("m_score")

        val rfmScoreDF: DataFrame = rfmDF.select($"userId", rColumn, fColumn, mColumn)

        /*
         * +------+-------+-------+-------+
         * |userId|r_score|f_score|m_score|
         * +------+-------+-------+-------+
         * |1     |2.0    |4.0    |5.0    |
         * |102   |2.0    |3.0    |5.0    |
         * |107   |2.0    |3.0    |5.0    |
         * +------+-------+-------+-------+
         */
        // rfmScoreDF.show(3, truncate = false)

        /*
         * 3 使用 RFM_SCORE 进行聚类，对用户进行分组
         */

        // 3.1 将特征转换为向量
        val vectorAssembler: VectorAssembler = new VectorAssembler()
            .setInputCols(Array("r_score", "m_score", "f_score"))
            .setOutputCol("raw_features")

        val featuresDF = vectorAssembler.transform(rfmScoreDF)
        featuresDF.cache()

        /*
         * 模型调优一：对特征数据进行处理：最大最小归一化
         */
        val scaler = new MinMaxScaler()
            .setInputCol("raw_features")
            .setOutputCol("features")

        // Compute summary statistics and generate MinMaxScalerModel
        val scaledData = scaler.fit(featuresDF).transform(featuresDF)

        /*
         * +------+-------+-------+-------+-------------+-------------+
         * |userId|r_score|f_score|m_score|raw_features |features     |
         * +------+-------+-------+-------+-------------+-------------+
         * |1     |2.0    |4.0    |5.0    |[2.0,5.0,4.0]|[1.0,1.0,1.0]|
         * |102   |2.0    |3.0    |5.0    |[2.0,5.0,3.0]|[1.0,1.0,0.5]|
         * |107   |1.0    |3.0    |5.0    |[1.0,5.0,3.0]|[0.0,1.0,0.5]|
         * +------+-------+-------+-------+-------------+-------------+
         */
        scaledData.show(3, truncate = false)

        // 调用 KMeans 算法，模型调优二：自动选择最佳的模型，模型调优三：加载或者训练模型
        val model = loadModel(scaledData)

        // 模型评估
        val predictions = model.transform(scaledData)
        val silhouette = new ClusteringEvaluator().evaluate(predictions)
        println("silhouette: " + silhouette)

        /*
         * +------+-------+-------+-------+-------------+----------+
         * |userId|r_score|f_score|m_score|features     |prediction|
         * +------+-------+-------+-------+-------------+----------+
         * |1     |2.0    |4.0    |5.0    |[2.0,5.0,4.0]|4         |
         * |102   |2.0    |3.0    |5.0    |[2.0,5.0,3.0]|1         |
         * |107   |2.0    |3.0    |5.0    |[2.0,5.0,3.0]|1         |
         * |110   |2.0    |3.0    |5.0    |[2.0,5.0,3.0]|1         |
         * |111   |2.0    |3.0    |5.0    |[2.0,5.0,3.0]|1         |
         * |120   |2.0    |3.0    |5.0    |[2.0,5.0,3.0]|1         |
         * |130   |2.0    |3.0    |4.0    |[2.0,4.0,3.0]|2         |
         * |135   |2.0    |3.0    |5.0    |[2.0,5.0,3.0]|1         |
         * |137   |2.0    |3.0    |5.0    |[2.0,5.0,3.0]|1         |
         * |139   |2.0    |4.0    |5.0    |[2.0,5.0,4.0]|4         |
         * +------+-------+-------+-------+-------------+----------+
         */
        // predictions.show(10, truncate = false)

        // 使用封装后的方法
        val indexTagMap = TagTools.convertIndexMap(model.clusterCenters, tagDF)

        //
        // /*
        //  * (0, [1.0,5.0,3.0291970802919708])
        //  * (1, [2.0,5.0,3.0])
        //  * (2, [2.0,4.0,2.9814814814814814])
        //  * (3, [1.0,4.0,2.9607843137254903])
        //  * (4, [2.0,5.0,4.0])
        //  */
        // val index = model.clusterCenters.zipWithIndex.map(t => (t._2, t._1))
        // index.foreach(println)
        //
        //
        // /*
        //  * 4 与 RFM 模型对应，对中心点的评分进行累加，从大到小排序即是从高价值到低价值
        //  *
        //  * (4,0)
        //  * (1,1)
        //  * (0,2)
        //  * (2,3)
        //  * (3,4)
        //  */
        // val rfmRule = index.map(t => (t._1, t._2.toArray.sum))
        //     .sortBy(_._2)
        //     .reverse
        //     .zipWithIndex
        //     .map(m => (m._1._1, m._2))
        // rfmRule.foreach(println)
        //
        // /*
        //  * 5 打标签
        //  */
        // val ruleMap = TagTools.convertMap(tagDF)
        //
        // /*
        //  * (0,高价值)
        //  * (1,中上价值)
        //  * (2,中价值)
        //  * (3,中下价值)
        //  * (4,超低价值)
        //  */
        // ruleMap.foreach(println)
        //
        // val indexTagMap = rfmRule.map {
        //     case (centerIndex, index) =>
        //         (centerIndex, ruleMap(index.toString))
        // }.toMap
        //
        // /*
        //  * (0, 中价值)
        //  * (1, 中上价值)
        //  * (2, 中下价值)
        //  * (3, 超低价值)
        //  * (4, 高价值)
        //  */
        // indexTagMap.foreach(println)

        // 使用预测的值进行打标签
        val indexTagMapBroad = spark.sparkContext.broadcast(indexTagMap)

        val index2tag = udf((index: Int) => {
            indexTagMapBroad.value(index)
        })

        /*
         * +------+--------+
         * |userId|rfm     |
         * +------+--------+
         * |1     |高价值   |
         * |102   |中上价值 |
         * |107   |中上价值 |
         * +------+--------+
         */
        val modelDF = predictions.select($"userId", index2tag($"prediction").as("rfm"))
        modelDF.show(3, truncate = false)

        modelDF
    }

    /*
     * 模型训练
     */
    def trainModel(featureDF: DataFrame): KMeansModel = {
        // 调用 KMeans 算法
        val kMeans = new KMeans().setK(5).setMaxIter(20).setSeed(1L).setInitMode("k-means||")
        val model = kMeans.fit(featureDF)

        model
    }

    /*
     * 通过调整超参数，获取最佳模型
     */
    def trainBestModel(featureDF: DataFrame): KMeansModel = {
        // 针对 KMeans 聚类算法来说，超参数有 K 值（肘部法则），但是在此例中 K 值已经确定为 5，所以此处主要调整最大迭代次数

        // 1.设置超参数的值
        val maxIters = Array(5, 10, 20, 50)

        // 2.不同超参数的值，训练模型
        val models: Array[(Double, KMeansModel, Int)] = maxIters.map {
            maxIter => {
                // a. 使用KMeans算法应用数据训练模式
                val kMeans: KMeans = new KMeans()
                    .setFeaturesCol("features")
                    .setPredictionCol("prediction")
                    .setK(5)
                    .setMaxIter(maxIter)
                    .setSeed(31)

                // b. 训练模式
                val model: KMeansModel = kMeans.fit(featureDF)

                // c. 模型评估指标 - 轮廓系数
                val predictions = model.transform(featureDF)
                val silhouette: Double = new ClusteringEvaluator().evaluate(predictions)

                // d. 返回三元组(评估指标, 模型, 超参数的值)
                (silhouette, model, maxIter)
            }
        }

        // (0.9950901810742503,KMeansModel: uid=kmeans_3330dee15e48, k=5, distanceMeasure=euclidean, numFeatures=3,5)
        // (0.9950901810742503,KMeansModel: uid=kmeans_95fd0829fc3e, k=5, distanceMeasure=euclidean, numFeatures=3,10)
        // (0.9950901810742503,KMeansModel: uid=kmeans_e9fa2db83068, k=5, distanceMeasure=euclidean, numFeatures=3,20)
        // (0.9950901810742503,KMeansModel: uid=kmeans_43f8a761dac3, k=5, distanceMeasure=euclidean, numFeatures=3,50)
        models.foreach(println)

        // 3.获取最佳模型
        val (_, bestModel, _) = models.maxBy(tuple => tuple._1)

        // 4.返回最佳模型
        bestModel
    }

    /*
     * 加载模型，当存在时从 Hdfs 加载，不存在的时候训练并保存
     */
    def loadModel(featuresDF: DataFrame): KMeansModel = {
        val modelPath: String = s"${ModelConfig.MODEL_BASE_PATH}/${this.getClass.getSimpleName.stripSuffix("$")}"
        val modelExists = HdfsUtils.exists(featuresDF.sparkSession.sparkContext.hadoopConfiguration, modelPath)

        // 模型存在则直接加载并返回
        if (modelExists) {
            println(s"从 Hdfs[$modelPath] 加载模型 ...")
            KMeansModel.load(modelPath)
        } else {
            println(s"训练模型并保存 [$modelPath] ...")
            val model = trainBestModel(featuresDF)
            model.save(modelPath)
            model
        }
    }

}

object RfmTagModel {
    def main(args: Array[String]): Unit = {
        val model = new RfmTagModel
        model.executeModel(361)
    }
}
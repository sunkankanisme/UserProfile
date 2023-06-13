package cn.itcast.tags.models.ml

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Column, DataFrame}

/*
 * 挖掘类型模型开发，客户价值 RFM
 */
class RfmModel extends AbstractModel("客户价值RFM", ModelType.ML) {

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
            .setOutputCol("features")

        val vectorDF = vectorAssembler.transform(rfmScoreDF)
        vectorDF.cache()

        /*
         * +------+-------+-------+-------+-------------+
         * |userId|r_score|f_score|m_score|features     |
         * +------+-------+-------+-------+-------------+
         * |1     |2.0    |4.0    |5.0    |[2.0,5.0,4.0]|
         * |102   |2.0    |3.0    |5.0    |[2.0,5.0,3.0]|
         * |107   |2.0    |3.0    |5.0    |[2.0,5.0,3.0]|
         * +------+-------+-------+-------+-------------+
         */
        // vectorDF.show(3, truncate = false)

        // 调用 KMeans 算法
        val model = trainModel(vectorDF)

        // 模型评估
        val predictions = model.transform(vectorDF)
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


}

object RfmModel {
    def main(args: Array[String]): Unit = {
        val model = new RfmModel
        model.executeModel(361)
    }
}
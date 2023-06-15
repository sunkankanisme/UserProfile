package cn.itcast.tags.models.ml

import cn.itcast.tags.config.ModelConfig
import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.{MLModelTools, TagTools}
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/*
 * 挖掘类型标签 - 用户活跃度 RFE 模型开发
 */
class RfeModel extends AbstractModel("用户活跃度 RFE", ModelType.ML) {
    override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
        val session = businessDF.sparkSession
        import session.implicits._
        
        // +--------------+---------------------------------------------------------------+-------------------+
        // |global_user_id|loc_url                                                        |log_time           |
        // +--------------+---------------------------------------------------------------+-------------------+
        // |424           |http://m.eshop.com/mobile/coupon/getCoupons.html?couponsId=3377|2019-08-13 03:03:55|
        // |619           |http://m.eshop.com/?source=mobile                              |2019-07-29 15:07:41|
        // |898           |http://m.eshop.com/mobile/item/11941.html                      |2019-08-14 09:23:44|
        // +--------------+---------------------------------------------------------------+-------------------+
        // businessDF.show(3, truncate = false)
        // +---+----------+----+-----+
        // |id |name      |rule|level|
        // +---+----------+----+-----+
        // |368|非常活跃   |0   |5    |
        // |369|活跃      |1   |5    |
        // |370|不活跃    |2   |5    |
        // |371|非常不活跃 |3   |5    |
        // +---+----------+----+-----+
        // tagDF.filter("level = 5").show(10, truncate = false)
        
        /*
         * 1 数据准备
         *
         * 计算R值：最近一次访问时间，距离今天的天数 - max -> datediff
         * 计算F值：所有访问浏览量（PV） - count
         * 计算E值：所有访问页面量（不包含重复访问页面）（UV） - count distinct
         */
        val rfeDF: DataFrame = businessDF.groupBy($"global_user_id")
            .agg(
                max($"log_time").as("last_time"),
                count($"loc_url").as("frequency"),
                countDistinct($"loc_url").as("engagements")
            )
            .select(
                $"global_user_id".as("userId"),
                datediff(current_timestamp(), $"last_time").as("recency"),
                $"frequency",
                $"engagements"
            )
        
        // +------+-------+---------+-----------+
        // |userId|recency|frequency|engagements|
        // +------+-------+---------+-----------+
        // |     1|   1398|      418|        270|
        // |   102|   1398|      415|        271|
        // |   107|   1398|      425|        281|
        // +------+-------+---------+-----------+
        // rfeDF.show(3)
        
        /*
         * 2 按照规则进行打分
         *
         * R: 0-15天=5分，16-30天=4分，31-45天=3分，46-60天=2分，大于61天=1分
         * F: ≥400=5分，300-399=4分，200-299=3分，100-199=2分，≤99=1分
         * E: ≥250=5分，200-249=4分，150-199=3分，149-50=2分，≤49=1分
         */
        // R 打分条件表达式
        val rWhen = when(col("recency").between(1, 15), 5.0)
            .when(col("recency").between(16, 30), 4.0)
            .when(col("recency").between(31, 45), 3.0)
            .when(col("recency").between(46, 60), 2.0)
            .when(col("recency").geq(61), 1.0)
        // F 打分条件表达式
        val fWhen = when(col("frequency").leq(99), 1.0)
            .when(col("frequency").between(100, 199), 2.0)
            .when(col("frequency").between(200, 299), 3.0)
            .when(col("frequency").between(300, 399), 4.0)
            .when(col("frequency").geq(400), 5.0)
        // E 打分条件表达式
        val eWhen = when(col("engagements").lt(49), 1.0)
            .when(col("engagements").between(50, 149), 2.0)
            .when(col("engagements").between(150, 199), 3.0)
            .when(col("engagements").between(200, 249), 4.0)
            .when(col("engagements").geq(250), 5.0)
        val rfeScoreDF: DataFrame = rfeDF.select(
            $"userId",
            rWhen.as("r_score"),
            fWhen.as("f_score"),
            eWhen.as("e_score")
        )
        
        // +------+-------+-------+-------+
        // |userId|r_score|f_score|e_score|
        // +------+-------+-------+-------+
        // |     1|    1.0|    5.0|    5.0|
        // |   102|    1.0|    5.0|    5.0|
        // |   167|    1.0|    4.0|    4.0|
        // |   168|    1.0|    5.0|    5.0|
        // |   178|    1.0|    5.0|    5.0|
        // |   179|    1.0|    5.0|    5.0|
        // +------+-------+-------+-------+
        // rfeScoreDF.show()
        
        /*
         * 3 根据打分进行模型开发
         */
        
        // 3.1 组合 RFE 特征为向量
        val assembler: VectorAssembler = new VectorAssembler()
            .setInputCols(Array("r_score", "f_score", "e_score"))
            .setOutputCol("features")
        val rfeFeaturesDF: DataFrame = assembler.transform(rfeScoreDF)
        
        // 3.2 构建 KMeans 最佳模型
        val modelPath: String = s"${ModelConfig.MODEL_BASE_PATH}/${this.getClass.getSimpleName.stripSuffix("$")}"
        val model: KMeansModel = MLModelTools.loadModel(rfeFeaturesDF, "kmeans", modelPath).asInstanceOf[KMeansModel]
        
        // 3.3 模型预测
        val predictionDF = model.transform(rfeFeaturesDF)
        // +------+-------+-------+-------+-------------+----------+
        // |userId|r_score|f_score|e_score|features     |prediction|
        // +------+-------+-------+-------+-------------+----------+
        // |1     |1.0    |5.0    |5.0    |[1.0,5.0,5.0]|2         |
        // |102   |1.0    |5.0    |5.0    |[1.0,5.0,5.0]|2         |
        // |107   |1.0    |5.0    |5.0    |[1.0,5.0,5.0]|2         |
        // |137   |1.0    |5.0    |5.0    |[1.0,5.0,5.0]|2         |
        // |139   |1.0    |4.0    |5.0    |[1.0,4.0,5.0]|0         |
        // +------+-------+-------+-------+-------------+----------+
        // predictionDF.show(10, truncate = false)
        
        /*
         * 4 打标签
         */
        
        // 4.1 获取中心点
        val clusterCenters: Array[linalg.Vector] = model.clusterCenters
        // [1.0,4.0,5.0] => 10
        // [1.0,5.0,4.0] => 10
        // [1.0,5.0,5.0] => 11
        // [1.0,4.0,4.0] => 9
        // clusterCenters.foreach(println)
        
        // 4.2 依据属性标签规则、模型类簇中心点以及预测值，打上标签
        val indexTagMap: Map[Int, String] = TagTools.convertIndexMap(clusterCenters, tagDF)
        val indexTagMapBroadcast = spark.sparkContext.broadcast(indexTagMap)
        
        // 4.3 自定义 UDF 函数，传递预测值 prediction，返回标签名称 tagName
        val index_to_tag = udf((clusterIndex: Int) => {
            indexTagMapBroadcast.value(clusterIndex)
        })
        
        // 4.4 使用 udf 打上标签
        val modelDF: DataFrame = predictionDF
            .select(
                $"userId",
                index_to_tag($"prediction").as("tagId")
            )
        
        // +------+----------+
        // |userId|     tagId|
        // +------+----------+
        // |     1|  非常活跃 |
        // |   102|  非常活跃 |
        // |   107|  非常活跃 |
        // |   110|      活跃|
        // |   111|      活跃|
        // |   120|      活跃|
        // |   130|    不活跃|
        // |   135|非常不活跃 |
        // |   137|  非常活跃 |
        // |   139|      活跃|
        // +------+----------+
        modelDF.show(10)
        
        modelDF
    }
}

object RfeModel {
    def main(args: Array[String]): Unit = {
        val model = new RfeModel()
        model.executeModel(367)
    }
}

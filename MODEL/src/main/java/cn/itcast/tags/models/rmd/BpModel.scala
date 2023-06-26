package cn.itcast.tags.models.rmd

import cn.itcast.tags.models.{AbstractModel, ModelType}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{concat_ws, udf}
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.apache.spark.storage.StorageLevel

import scala.util.matching.Regex

/*
 * 推荐系统模型开发，使用 ALS 完成向用户推荐物品的功能
 */
class BpModel extends AbstractModel("用户购物偏好（商品/品牌偏好）", ModelType.ML) {
    override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
        import businessDF.sparkSession.implicits._
        
        // +--------------+---------------------------------------------------------------+
        // |global_user_id|loc_url                                                        |
        // +--------------+---------------------------------------------------------------+
        // |424           |http://m.eshop.com/mobile/coupon/getCoupons.html?couponsId=3377|
        // |619           |http://m.eshop.com/?source=mobile                              |
        // |898           |http://m.eshop.com/mobile/item/11941.html                      |
        // +--------------+---------------------------------------------------------------+
        // businessDF.show(3, truncate = false)
        
        /*
         * 通过用户行为日志，获取用户点击商品的次数
         * 1 从 url 中提取出 productId - udf
         * 2 过滤出 productId 不为 null 的
         * 3 按照 userId 和 productId 分组，计算出用户对产品的点击次数 clickCount，作为评分（隐式评价）
         * 4 使用 ALS 训练模型，使用交叉验证或者训练分割验证，调整超参数，获取作假模型
         * 5 使用模型为用户推荐物品，并将推荐的数据存入 HBase 表中
         */
        
        
        // 1. 自定义函数，从url中提取出访问商品id
        val url_to_product: UserDefinedFunction = udf(
            (url: String) => {
                // 正则表达式
                val regex: Regex = "^.+/product/(\\d+)\\.html.+$".r
                
                // 正则匹配
                val optionMatch: Option[Regex.Match] = regex.findFirstMatchIn(url)
                
                // 获取匹配的值
                val productId = optionMatch match {
                    case Some(matchValue) => matchValue.group(1)
                    case None => null
                }
                
                // 返回productId
                productId
            }
        )
        
        // 2. 从url中计算商品id
        val ratingsDF: DataFrame = businessDF
            // 获取loc_url不为null
            .filter($"loc_url".isNotNull)
            .select(
                $"global_user_id".as("userId"),
                url_to_product($"loc_url").as("productId")
            )
            // 过滤不为空的数据
            .filter($"productId".isNotNull)
            // 统计每个用户点击各个商品的次数
            .groupBy($"userId", $"productId")
            .count()
            // 数据类型的转换
            .select(
                $"userId".cast(DoubleType),
                $"productId".cast(DoubleType),
                $"count".as("rating").cast(DoubleType)
            )
        
        // +------+---------+------+
        // |userId|productId|rating|
        // +------+---------+------+
        // |530.0 |5543.0   |1.0   |
        // |498.0 |7147.0   |1.0   |
        // |153.0 |11111.0  |1.0   |
        // |278.0 |11589.0  |1.0   |
        // |397.0 |11949.0  |1.0   |
        // |662.0 |5903.0   |1.0   |
        // |192.0 |5884.0   |1.0   |
        // |43.0  |10611.0  |1.0   |
        // |480.0 |4840.0   |1.0   |
        // |368.0 |10607.0  |1.0   |
        // +------+---------+------+
        // ratingsDF.show(10, truncate = false)
        
        // 数据缓存
        ratingsDF.persist(StorageLevel.MEMORY_AND_DISK)
        
        // 3. 使用ALS算法训练模型（评分为隐式评分）
        val alsModel: ALSModel = new ALS()
            // 设置属性
            .setUserCol("userId")
            .setItemCol("productId")
            .setRatingCol("rating")
            // 设置算法参数
            // 隐式评分
            .setImplicitPrefs(true)
            // 矩阵因子，rank秩的值
            .setRank(10)
            // 最大迭代次数
            .setMaxIter(10)
            // 冷启动
            .setColdStartStrategy("drop")
            .setAlpha(1.0)
            .setRegParam(1.0)
            // 应用数据集，训练模型
            .fit(ratingsDF)
        
        ratingsDF.unpersist()
        
        // 4. 模型评估
        val evaluator: RegressionEvaluator = new RegressionEvaluator()
            .setLabelCol("rating")
            .setPredictionCol("prediction")
            .setMetricName("rmse")
        val rmse: Double = evaluator.evaluate(alsModel.transform(ratingsDF))
        
        // rmse = 1.0300179223678094
        println(s"rmse = $rmse")
        
        // 5.1 给用户推荐商品: Top5
        val rmdItemsDF: DataFrame = alsModel.recommendForAllUsers(5)
        // +------+------------------------------------------------------------------------------------------------------+
        // |userId|recommendations                                                                                       |
        // +------+------------------------------------------------------------------------------------------------------+
        // |12    |[[6603, 0.18910232], [10935, 0.18785408], [9371, 0.18159339], [6395, 0.16834861], [11949, 0.1611611]] |
        // |13    |[[6603, 0.24412209], [10935, 0.24251066], [9371, 0.23442838], [6395, 0.21733002], [11949, 0.20805128]]|
        // |14    |[[6603, 0.21862397], [10935, 0.21718085], [9371, 0.20994276], [6395, 0.19463028], [11949, 0.18632069]]|
        // |18    |[[6603, 0.21503523], [10935, 0.21361582], [9371, 0.20649654], [6395, 0.19143541], [11949, 0.18326223]]|
        // |25    |[[6603, 0.24712016], [10935, 0.24548894], [9371, 0.2373074], [6395, 0.21999907], [11949, 0.21060637]] |
        // |37    |[[6603, 0.21493474], [10935, 0.21351598], [9371, 0.20640004], [6395, 0.19134596], [11949, 0.18317659]]|
        // |38    |[[6603, 0.21453734], [10935, 0.2131212], [9371, 0.20601842], [6395, 0.19099218], [11949, 0.1828379]]  |
        // |46    |[[6603, 0.25761515], [10935, 0.25591466], [9371, 0.24738567], [6395, 0.22934227], [11949, 0.21955068]]|
        // |50    |[[6603, 0.21381176], [10935, 0.21240039], [9371, 0.20532164], [6395, 0.1903462], [11949, 0.18221952]] |
        // |52    |[[6603, 0.2332502], [10935, 0.23171052], [9371, 0.2239882], [6395, 0.20765132], [11949, 0.19878581]]  |
        // +------+------------------------------------------------------------------------------------------------------+
        // rmdItemsDF.show(10, truncate = false)
        
        // 5.2. 给物品推荐用户
        val rmdUsersDF: DataFrame = alsModel.recommendForAllItems(5)
        // +---------+--------------------------------------------------------------------------------------------------------+
        // |productId|recommendations                                                                                         |
        // +---------+--------------------------------------------------------------------------------------------------------+
        // |408      |[[918, 0.076566465], [947, 0.07627316], [168, 0.07578477], [797, 0.07481006], [665, 0.074542224]]       |
        // |624      |[[918, 0.0028913536], [947, 0.0028802776], [168, 0.0028618348], [797, 0.0028250269], [665, 0.002814913]]|
        // |1843     |[[918, 0.047865607], [947, 0.04768225], [168, 0.047376934], [797, 0.046767585], [665, 0.04660015]]      |
        // |2123     |[[918, 0.0046099364], [947, 0.004592277], [168, 0.0045628725], [797, 0.0045041856], [665, 0.0044880603]]|
        // |2127     |[[918, 0.005198319], [947, 0.005178406], [168, 0.0051452485], [797, 0.0050790715], [665, 0.0050608874]] |
        // |2184     |[[918, 0.0024695327], [947, 0.0024600725], [168, 0.0024443204], [797, 0.002412882], [665, 0.0024042437]]|
        // |2386     |[[918, 0.020867497], [947, 0.02078756], [168, 0.020654453], [797, 0.020388799], [665, 0.020315805]]     |
        // |2501     |[[918, 0.060240168], [947, 0.060009412], [168, 0.059625164], [797, 0.058858283], [665, 0.058647558]]    |
        // |2585     |[[918, 0.046119567], [947, 0.0459429], [168, 0.045648724], [797, 0.045061596], [665, 0.044900272]]      |
        // |2728     |[[918, 0.001686305], [947, 0.0016798453], [168, 0.0016690892], [797, 0.0016476217], [665, 0.0016417231]]|
        // +---------+--------------------------------------------------------------------------------------------------------+
        // rmdUsersDF.show(10, truncate = false)
        
        // 6 存储推荐的数据
        val modelDF: DataFrame = rmdItemsDF
            .select(
                $"userId",
                $"recommendations.productId".as("productIds"),
                $"recommendations.rating".as("ratings")
            )
            .select(
                $"userId".cast(StringType),
                concat_ws(",", $"productIds").as("productIds"),
                concat_ws(",", $"ratings").as("ratings")
            )
        
        // +------+--------------------------+------------------------------------------------------+
        // |userId|productIds                |ratings                                               |
        // +------+--------------------------+------------------------------------------------------+
        // |12    |6603,10935,9371,6395,11949|0.18910232,0.18785408,0.18159339,0.16834861,0.1611611 |
        // |13    |6603,10935,9371,6395,11949|0.24412209,0.24251066,0.23442838,0.21733002,0.20805128|
        // |14    |6603,10935,9371,6395,11949|0.21862397,0.21718085,0.20994276,0.19463028,0.18632069|
        // |18    |6603,10935,9371,6395,11949|0.21503523,0.21361582,0.20649654,0.19143541,0.18326223|
        // |25    |6603,10935,9371,6395,11949|0.24712016,0.24548894,0.2373074,0.21999907,0.21060637 |
        // |37    |6603,10935,9371,6395,11949|0.21493474,0.21351598,0.20640004,0.19134596,0.18317659|
        // |38    |6603,10935,9371,6395,11949|0.21453734,0.2131212,0.20601842,0.19099218,0.1828379  |
        // |46    |6603,10935,9371,6395,11949|0.25761515,0.25591466,0.24738567,0.22934227,0.21955068|
        // |50    |6603,10935,9371,6395,11949|0.21381176,0.21240039,0.20532164,0.1903462,0.18221952 |
        // |52    |6603,10935,9371,6395,11949|0.2332502,0.23171052,0.2239882,0.20765132,0.19878581  |
        // +------+--------------------------+------------------------------------------------------+
        // modelDF.show(10, truncate = false)
        
        // 7. 保存推荐的结果到HBase表中
        modelDF.write
            .mode(SaveMode.Overwrite)
            .format("hbase")
            .option("zkHosts", "hadoop101")
            .option("zkPort", "2181")
            .option("hbaseTable", "tbl_rmd_items")
            .option("family", "info")
            .option("rowKeyColumn", "userId")
            .save()
        
        null
    }
}

object BpModel extends App {
    private val bpModel = new BpModel()
    bpModel.executeModel(382)
}

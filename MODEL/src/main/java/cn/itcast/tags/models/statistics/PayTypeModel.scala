package cn.itcast.tags.models.statistics

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window

/*
 * 统计类型标签-支付方式模型
 */
class PayTypeModel extends AbstractModel("支付方式", ModelType.STATISTICS) {
    override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
        import businessDF.sparkSession.implicits._
        import org.apache.spark.sql.functions._

        /*
         * +--------+-----------+
         * |memberid|paymentcode|
         * +--------+-----------+
         * |     265|     alipay|
         * |     208|     alipay|
         * |     673|     alipay|
         * |      54|        cod|
         * |     841|     alipay|
         * |     241|     alipay|
         * |     582|     alipay|
         * +--------+-----------+
         */
        // businessDF.printSchema()
        businessDF.show()

        /*
         * ++---+--------+--------------------+-----+
         * | id|    name|                rule|level|
         * +---+--------+--------------------+-----+
         * |357|  支付宝|              alipay|    5|
         * |358|微信支付|               wxpay|    5|
         * |359|银联支付|            chinapay|    5|
         * |360|货到付款|                 cod|    5|
         * +---+--------+--------------------+-----+
         */
        // tagDF.printSchema()
        tagDF.show()

        // 统计每个用户使用最多的支付方式
        val paymentDF = businessDF
            .groupBy("memberid", "paymentcode")
            .count()
            .withColumn(
                "rnk",
                rank().over(
                    Window.partitionBy($"memberid").orderBy($"count".desc)
                )
            )
            .where($"rnk" === 1)
            .select($"memberid".as("id"), $"paymentcode".as("payment"))

        /*
         * +---+-----------+
         * | id|paymentcode|
         * +---+-----------+
         * |  1|     alipay|
         * |168|     alipay|
         * |178|     alipay|
         * |179|     alipay|
         * +---+-----------+
         */
        paymentDF.printSchema()
        // paymentDF.show()

        // 使用属性标签规则匹配方法进行打标签
        val modelDF = TagTools.ruleMatchTag(paymentDF, "payment", tagDF)

        /*
         * +------+-------+
         * |userId|payment|
         * +------+-------+
         * |     1| 支付宝|
         * |   102| 支付宝|
         * |   107| 支付宝|
         * |   110| 支付宝|
         * |   111| 支付宝|
         * |   120| 支付宝|
         * |   178| 支付宝|
         * |   179| 支付宝|
         * +------+-------+
         */
        modelDF.show()

        modelDF
    }
}

object PayTypeModel extends App {
    private val model = new PayTypeModel
    model.executeModel(356)
}

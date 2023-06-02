package cn.itcast.tags.models.statistics

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame

class ConsumeCycleModel extends AbstractModel("消费周期", ModelType.STATISTICS) {
    override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
        import businessDF.sparkSession.implicits._
        import org.apache.spark.sql.functions._

        /*
         * +---------+----------+
         * | memberid|finishtime|
         * +---------+----------+
         * |     265|1659715200 |
         * |     208|1662566400 |
         * |     673|1664640000 |
         * |     584|1666108800 |
         * +---------+----------+
         */
        businessDF.printSchema()
        // businessDF.show()

        /*
         * +---+--------+--------------------+-----+
         * | id|    name|                rule|level|
         * +---+--------+--------------------+-----+
         * |348|   近7天|                 0-7|    5|
         * |349|   近2周|                8-14|    5|
         * |350|   近1月|               15-30|    5|
         * |351|   近2月|               31-60|    5|
         * |352|   近3月|               61-90|    5|
         * |353|   近4月|              91-120|    5|
         * |354|   近5月|             121-150|    5|
         * |355|  近半年|             151-180|    5|
         * +---+--------+--------------------+-----+
         */
        tagDF.printSchema()
        // tagDF.show()

        // 业务数据计算
        val consumerDaysDF = businessDF
            // .withColumn("finishtime", $"finishtime".cast(LongType))
            .groupBy($"memberid")
            .agg(max("finishtime").as("finish_time"))
            .select(
                $"memberid",
                from_unixtime($"finish_time").as("finish_time"),
                current_timestamp().as("now_time"))
            .select(
                $"memberid".as("id"),
                datediff($"now_time", $"finish_time").as("consumer_days")
            )

        /*
         * +---+-------------+
         * |id |consumer_days|
         * +---+-------------+
         * |1  |1            |
         * |102|1            |
         * |107|4            |
         * |167|5            |
         * |168|5            |
         * |178|2            |
         * |179|5            |
         * +---+-------------+
         */
        consumerDaysDF.printSchema()
        consumerDaysDF.show(false)

        // 使用属性标签规则数据，打标签
        val attrTagRuleDF = TagTools.convertTuple(tagDF)

        // 关联df
        val modelDF = consumerDaysDF.join(attrTagRuleDF)
            .where($"consumer_days" between($"start", $"end"))
            .select($"id".as("userId"), $"name".as("consumercycle"))

        /*
         * +------+-------------+
         * |userId|consumercycle|
         * +------+-------------+
         * |     1|        近7天|
         * |   102|        近7天|
         * |   107|        近7天|
         * |   110|        近7天|
         * |   111|        近7天|
         * |   120|        近7天|
         * |   130|        近7天|
         * |   135|        近7天|
         * |   137|        近7天|
         * |   139|        近7天|
         * +------+-------------+
         */
        modelDF.printSchema()
        modelDF.show(100)

        modelDF
    }
}

object ConsumeCycleModel extends App {
    private val model = new ConsumeCycleModel
    model.executeModel(347)
}

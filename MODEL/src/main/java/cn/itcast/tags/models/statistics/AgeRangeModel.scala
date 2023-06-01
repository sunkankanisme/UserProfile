package cn.itcast.tags.models.statistics

import cn.itcast.tags.models.{AbstractModel, ModelType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

/*
 * 统计类型标签-年龄段模型
 */
class AgeRangeModel extends AbstractModel("年龄范围", ModelType.STATISTICS) {
    override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
        import businessDF.sparkSession.implicits._

        /*
         * +---+----------+
         * | id|  birthday|
         * +---+----------+
         * |  1|1992-05-31|
         * | 10|1980-10-13|
         * |100|1993-10-28|
         * |101|1996-08-18|
         * |115|2001-01-10|
         * |116|1977-04-06|
         * +---+----------+
         */
        businessDF.printSchema()
        // businessDF.show()

        /*
         * +---+------+--------------------+-----+
         * | id|  name|                rule|level|
         * +---+------+--------------------+-----+
         * |339|  50后|   19500101-19591231|    5|
         * |340|  60后|   19600101-19691231|    5|
         * |341|  70后|   19700101-19791231|    5|
         * |342|  80后|   19800101-19891231|    5|
         * |343|  90后|   19900101-19991231|    5|
         * |344|  00后|   20000101-20091231|    5|
         * |345|  10后|   20100101-20191231|    5|
         * |346|  20后|   20200101-20291231|    5|
         * +---+------+--------------------+-----+
         */
        tagDF.printSchema()
        // tagDF.show()

        // 自定义 udf 解析属性标签的规则 rule
        val rule_to_tuple = udf(
            (rule: String) => {
                val Array(start, end) = rule.trim.split("-").map(_.toInt)
                (start, end)
            }
        )

        // 针对属性标签数据中的规则 rule 进行使用 udf 函数
        val attrTagRuleDF = tagDF.filter($"level" === 5)
            .select($"name", rule_to_tuple($"rule") as "rules")
            .select($"name", $"rules._1".as("start"), $"rules._2".as("end"))

        // 使用业务数据字段 birthday 与属性标签规则字段进行关联
        val modelDF = businessDF
            .select($"id", regexp_replace($"birthday", "-", "").cast(IntegerType).as("bornDate"))
            .join(attrTagRuleDF)
            .where($"bornDate" between($"start", $"end"))
            .select($"id".as("userId"), $"name".as("agerange"))

        /*
         * +------+--------+
         * |userId|agerange|
         * +------+--------+
         * |     1|    90后|
         * |    10|    80后|
         * |   100|    90后|
         * |   101|    90后|
         * |   116|    70后|
         * +------+--------+
         */
        modelDF.printSchema()
        // modelDF.show()

        modelDF
    }
}

object AgeRangeModel {
    def main(args: Array[String]): Unit = {
        val model = new AgeRangeModel
        model.executeModel(338)
    }
}
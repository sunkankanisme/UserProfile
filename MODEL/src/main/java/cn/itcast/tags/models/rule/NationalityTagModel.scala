package cn.itcast.tags.models.rule

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame

class NationalityTagModel extends AbstractModel("国籍标签", ModelType.MATCH) {

    override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
        /*
         * +---+-----------+
         * | id|nationality|
         * +---+-----------+
         * |  1|          1|
         * | 10|          1|
         * | 11|          1|
         * |116|          1|
         * +---+-----------+
         */
        businessDF.printSchema()
        // businessDF.show()

        /*
         * +---+--------+--------------------+-----+
         * | id|    name|                rule|level|
         * +---+--------+--------------------+-----+
         * |332|    国籍|inType=hbase
         * zkHo...|    4|
         * |333|中国大陆|                   1|    5|
         * |334|中国香港|                   2|    5|
         * |335|中国澳门|                   3|    5|
         * |336|中国台湾|                   4|    5|
         * |337|    其他|                   5|    5|
         * +---+--------+--------------------+-----+
         */
        tagDF.printSchema()
        // tagDF.show()

        val modelDF = TagTools.ruleMatchTag(businessDF, "nationality", tagDF)
        modelDF.printSchema()
        // modelDF.show()

        modelDF
    }

}

object NationalityTagModel {
    def main(args: Array[String]): Unit = {
        val tagModel = new NationalityTagModel()
        tagModel.executeModel(332)
    }
}

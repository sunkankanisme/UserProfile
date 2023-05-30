package cn.itcast.tags.models.rule

import cn.itcast.tags.models.BasicModel
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame

/*
 * 规则匹配标签-政治面貌模型
 */
class PoliticalModel extends BasicModel {
    override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
        val session = businessDF.sparkSession

        // businessDF.printSchema()
        // businessDF.show()

        // tagDF.printSchema()
        // tagDF.show()

        val modelDF = TagTools.ruleMatchTag(businessDF, "politicalface", tagDF)
        modelDF.printSchema()
        // modelDF.show()

        modelDF
    }
}

object PoliticalModel {
    def main(args: Array[String]): Unit = {
        val tagModel = new PoliticalModel
        tagModel.executeModel(328)
    }
}

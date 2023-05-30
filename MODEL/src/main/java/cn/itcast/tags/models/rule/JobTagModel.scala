package cn.itcast.tags.models.rule

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame

class JobTagModel extends AbstractModel("职业标签", ModelType.MATCH) {

    override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
        // businessDF.printSchema()
        // businessDF.show()

        // tagDF.printSchema()
        // tagDF.show()

        val modelDF = TagTools.ruleMatchTag(businessDF, "job", tagDF)
        // modelDF.printSchema()
        // modelDF.show()

        modelDF
    }

}

object JobTagModel {
    def main(args: Array[String]): Unit = {
        val model = new JobTagModel
        model.executeModel(321, isHive = true)
    }
}

package cn.itcast.tags.models.rule

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame

class GenderTagModel extends AbstractModel("性别标签", ModelType.MATCH) {
    override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
        TagTools.ruleMatchTag(businessDF, "gender", tagDF)
    }
}

object GenderTagModel {
    def main(args: Array[String]): Unit = {
        val model = new GenderTagModel
        model.executeModel(318)
    }
}

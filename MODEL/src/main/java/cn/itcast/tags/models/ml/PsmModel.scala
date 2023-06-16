package cn.itcast.tags.models.ml

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.{MLModelTools, TagTools}
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

/*
 * 使用 KMeans 算法训练 PSM 价格敏感度模型
 */
class PsmModel extends AbstractModel("消费敏感度", ModelType.ML) {
    override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
        import businessDF.sparkSession.implicits._
        
        // +--------+--------------------+-----------+---------------+
        // |memberid|ordersn             |orderamount|couponcodevalue|
        // +--------+--------------------+-----------+---------------+
        // |265     |gome_792756751164275|2479.45    |0.00           |
        // |208     |jd_14090106121770839|2449.00    |0.00           |
        // |673     |jd_14090112394810659|1099.42    |0.00           |
        // +--------+--------------------+-----------+---------------+
        
        businessDF.show(3, truncate = false)
        // +---+----------+----+-----+
        // |id |name      |rule|level|
        // +---+----------+----+-----+
        // |373|极度敏感  |0   |5     |
        // |374|比较敏感  |1   |5     |
        // |375|一般敏感  |2   |5     |
        // |376|不太敏感  |3   |5     |
        // |377|极度不敏感|4   |5     |
        // +---+----------+----+-----+
        tagDF.filter("level = 5").show(10, truncate = false)
        
        /*
         * 1 计算 PSM
         */
        
        // 1.1 计算指标
        // ra: receivableAmount 应收金额
        val raColumn: Column = ($"orderamount" + $"couponcodevalue").as("ra")
        // da: discountAmount 优惠金额
        val daColumn: Column = $"couponcodevalue".cast(DataTypes.createDecimalType(10, 2)).as("da")
        // pa: practicalAmount 实收金额
        val paColumn: Column = $"orderamount".cast(DataTypes.createDecimalType(10, 2)).as("pa")
        
        // state: 订单状态，此订单是否是优惠订单，0表示非优惠订单，1表示优惠订单
        val stateColumn: Column = when($"couponcodevalue" === 0.0, 0).otherwise(1).as("state")
        // tdon 优惠订单数
        val tdonColumn: Column = sum($"state").as("tdon")
        // ton 总订单总数
        val tonColumn: Column = count($"state").as("ton")
        // tda 优惠总金额
        val tdaColumn: Column = sum($"da").as("tda")
        // tra 应收总金额
        val traColumn: Column = sum($"ra").as("tra")
        
        // tdonr 优惠订单占比(优惠订单数 / 订单总数)
        // tdar 优惠总金额占比(优惠总金额 / 订单总金额)
        // adar 平均优惠金额占比(平均优惠金额 / 平均每单应收金额)
        val tdonrColumn: Column = ($"tdon" / $"ton").as("tdonr")
        val tdarColumn: Column = ($"tda" / $"tra").as("tdar")
        val adarColumn: Column = (($"tda" / $"tdon") / ($"tra" / $"ton")).as("adar")
        val psmColumn: Column = ($"tdonr" + $"tdar" + $"adar").as("psm")
        
        // 1.2 业务数据（订单数据）计算PSM值
        val psmDF: DataFrame = businessDF
            // 确定每个订单ra、da、pa及是否为优惠订单
            .select($"memberid".as("userId"), raColumn, daColumn, paColumn, stateColumn)
            // 按照userId分组，聚合统计：订单总数和订单总额
            .groupBy($"userId")
            .agg(tonColumn, tdonColumn, traColumn, tdaColumn)
            // 计算优惠订单占比、优惠总金额占比、adar
            .select($"userId", tdonrColumn, tdarColumn, adarColumn)
            // 计算PSM值
            .select($"*", psmColumn)
            .select(
                $"*",
                when($"psm".isNull, 0.00000001).otherwise($"psm").as("psm_score")
            )
        
        // +------+--------------------+---------------------+-------------------+-------------------+-------------------+
        // |userId|tdonr               |tdar                 |adar               |psm                |psm_score          |
        // +------+--------------------+---------------------+-------------------+-------------------+-------------------+
        // |149   |0.007692307692307693|8.82491881736557E-4  |0.11472394462575242|0.12329874419979667|0.12329874419979667|
        // |814   |0.029850746268656716|0.0028111584295449376|0.0941738073897554 |0.12683571208795708|0.12683571208795708|
        // |307   |0.02142857142857143 |0.0023032038948099143|0.10748284842446267|0.131214623747844  |0.131214623747844  |
        // |582   |0.022727272727272728|0.0020775818941231026|0.09141360334141652|0.11621845796281235|0.11621845796281235|
        // |633   |0.023622047244094488|0.0028414966295350935|0.12029002398365228|0.14675356785728186|0.14675356785728186|
        // +------+--------------------+---------------------+-------------------+-------------------+-------------------+
        psmDF.show(5, truncate = false)
        
        /*
         * 2 使用 KMeans 训练模型
         */
        val psmFeaturesDF: DataFrame = new VectorAssembler()
            .setInputCols(Array("psm_score"))
            .setOutputCol("features")
            .transform(psmDF)
        
        val kmeansModel: KMeansModel = MLModelTools.loadModel(psmFeaturesDF, "psm", this.getClass).asInstanceOf[KMeansModel]
        val predictionDF = kmeansModel.transform(psmFeaturesDF)
        // +------+--------------------+---------------------+-------------------+-------------------+-------------------+---------------------+----------+
        // |userId|tdonr               |tdar                 |adar               |psm                |psm_score          |features             |prediction|
        // +------+--------------------+---------------------+-------------------+-------------------+-------------------+---------------------+----------+
        // |149   |0.007692307692307693|8.82491881736557E-4  |0.11472394462575242|0.12329874419979667|0.12329874419979667|[0.12329874419979667]|0         |
        // |814   |0.029850746268656716|0.0028111584295449376|0.0941738073897554 |0.12683571208795708|0.12683571208795708|[0.12683571208795708]|0         |
        // |307   |0.02142857142857143 |0.0023032038948099143|0.10748284842446267|0.131214623747844  |0.131214623747844  |[0.131214623747844]  |0         |
        // +------+--------------------+---------------------+-------------------+-------------------+-------------------+---------------------+----------+
        predictionDF.show(3, truncate = false)
        
        /*
         * 3 使用模型和属性标签规则打标签
         */
        val modelDF: DataFrame = TagTools.kmeansMatchTag(kmeansModel, predictionDF, tagDF, "psm")
        // +------+--------+
        // |userId|psm     |
        // +------+--------+
        // |149   |一般敏感|
        // |814   |一般敏感|
        // |307   |一般敏感|
        // +------+--------+
        modelDF.show(3, truncate = false)
        
        modelDF
    }
}

object PsmModel {
    def main(args: Array[String]): Unit = {
        val model = new PsmModel()
        model.executeModel(372)
    }
}

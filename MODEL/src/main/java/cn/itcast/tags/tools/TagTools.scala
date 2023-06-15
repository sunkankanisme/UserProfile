package cn.itcast.tags.tools

import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.linalg
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 针对标签进行相关操作工具类
  */
object TagTools {
    
    /**
      * 将[属性标签]数据中[规则：rule与名称：name]转换为[Map集合]
      *
      * @param tagDF 属性标签数据
      *
      * @return Map 集合
      */
    def convertMap(tagDF: DataFrame): Map[String, String] = {
        import tagDF.sparkSession.implicits._
        tagDF
            // 获取属性标签数据
            .filter($"level" === 5)
            // 选择标签规则rule和标签Id
            .select($"rule", $"name")
            // 转换为Dataset
            .as[(String, String)]
            // 转换为RDD
            .rdd
            // 转换为Map集合
            .collectAsMap().toMap
    }
    
    /**
      * 依据[标签业务字段的值]与[标签规则]匹配，进行打标签（userId, tagName)
      *
      * @param dataframe 标签业务数据
      * @param field     标签业务字段
      * @param tagDF     标签数据
      *
      * @return 标签模型数据
      */
    def ruleMatchTag(dataframe: DataFrame, field: String, tagDF: DataFrame) = {
        val spark: SparkSession = dataframe.sparkSession
        import spark.implicits._
        
        // 获取规则rule与tagId集合
        val attrTagRuleMap: Map[String, String] = convertMap(tagDF)
        
        // 将 Map 集合数据广播出去
        val attrTagRuleMapBroadcast = spark.sparkContext.broadcast(attrTagRuleMap)
        
        // 自定义UDF函数, 依据Job职业和属性标签规则进行标签化
        val field_to_tag: UserDefinedFunction = udf(
            (field: String) => attrTagRuleMapBroadcast.value(field)
        )
        
        // 计算标签，依据业务字段值获取标签ID
        val modelDF: DataFrame = dataframe.select(
            $"id".as("userId"),
            field_to_tag(col(field)).as(field)
        )
        
        // modelDF.printSchema()
        // modelDF.show(50, truncate = false)
        
        // 5. 返回计算标签数据
        modelDF
    }
    
    /**
      * 将标签数据中属性标签规则rule拆分为范围: start, end
      *
      * @param tagDF 标签数据
      *
      * @return 数据集DataFrame
      */
    def convertTuple(tagDF: DataFrame): DataFrame = {
        // 导入隐式转换和函数库
        import tagDF.sparkSession.implicits._
        import org.apache.spark.sql.functions._
        
        // 1. 自定UDF函数，解析分解属性标签的规则rule： 19500101-19591231
        val rule_to_tuple: UserDefinedFunction = udf(
            (rule: String) => {
                val Array(start, end) = rule.split("-").map(_.toInt)
                // 返回二元组
                (start, end)
            }
        )
        
        // 2. 获取属性标签数据，解析规则rule
        val ruleDF: DataFrame = tagDF
            .filter($"level" === 5)
            .select(
                $"name",
                rule_to_tuple($"rule").as("rules")
            )
            // 获取起始start和结束end
            .select(
                $"name",
                $"rules._1".as("start"),
                $"rules._2".as("end")
            )
        
        // 3. 返回标签规则
        ruleDF
    }
    
    /**
      * 将KMeans模型中类簇中心点索引对应到属性标签的标签ID
      *
      * @param clusterCenters KMeans模型类簇中心点
      * @param tagDF          属性标签数据
      */
    def convertIndexMap(clusterCenters: Array[linalg.Vector], tagDF: DataFrame): Map[Int, String] = {
        // 1. 获取属性标签（5级标签）数据，选择Name和rule
        val rulesMap: Map[String, String] = convertMap(tagDF)
        
        // 2. 获取聚类模型中簇中心及索引
        val centerIndexArray: Array[((Int, Double), Int)] = clusterCenters
            .zipWithIndex
            .map { case (vector, centerIndex) => (centerIndex,
                vector.toArray.sum)
            }
            .sortBy { case (_, score) => -score }
            .zipWithIndex
        
        // 3. 聚类类簇关联属性标签属性rule，对应聚类类簇与标签tagName
        val indexTagMap: Map[Int, String] = centerIndexArray
            .map { case ((centerIndex, _), index) =>
                val tagName = rulesMap(index.toString)
                (centerIndex, tagName)
            }.toMap
        
        // 4. 返回类簇索引对应TagId的Map集合
        indexTagMap
    }
    
    /**
      * KMeans 算法标签模型基于属性规则标签数据和KMeansModel预测值打标签封装方法
      *
      * @param kmeansModel KMeans 算法模型
      * @param dataframe   预测数据集，含预测列prediction
      * @param tagDF       标签数据
      *
      * @return 用户标签数据，含uid和tagId列
      */
    def kmeansMatchTag(kmeansModel: KMeansModel, dataframe: DataFrame, tagDF: DataFrame, tagColumn: String): DataFrame = {
        val spark: SparkSession = dataframe.sparkSession
        import spark.implicits._
        
        // 1. 获取聚类模型中簇中心及索引
        val clusterCenters: Array[linalg.Vector] = kmeansModel.clusterCenters
        
        // 2. 类簇中心点对应标签ID
        val indexTagMap: Map[Int, String] = TagTools.convertIndexMap(clusterCenters, tagDF)
        val indexTagMapBroadcast = spark.sparkContext.broadcast(indexTagMap)
        
        // 3. 关联聚类预测值，将prediction转换为tagId
        val index_to_tag = udf(
            (prediction: Int) => indexTagMapBroadcast.value(prediction)
        )
        
        dataframe.select(
            $"userId",
            index_to_tag($"prediction").as(tagColumn)
        )
    }
    
}

package cn.itcast.tags.meta

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 加载业务数据工具类：
  *
  * 解析业务标签规则rule，依据规则判断数段数据源，加载业务数据
  */
object MetaParse extends Logging {
    
    /**
      * 依据标签数据，获取业务标签规则rule，解析转换为Map集合
      *
      * @param tagDF 标签数据
      *
      * @return Map集合
      */
    def parseRuleToParams(tagDF: DataFrame): Map[String, String] = {
        import tagDF.sparkSession.implicits._
        // 1. 4级标签规则rule
        val tagRule: String = tagDF
            .filter($"level" === 4)
            .head().getAs[String]("rule")
        
        logInfo(s"==== 业务标签数据规则: {$tagRule} ====")
        
        // 2. 解析标签规则，先按照换行\n符分割，再按照等号=分割
        /*
         * inType=hbase
         * zkHosts=hadoop101
         * zkPort=2181
         * hbaseTable=tbl_tag_logs
         * family=detail
         * selectFieldNames=global_user_id,loc_url,log_time
         * whereCondition=log_time#day#30
         */
        val paramsMap: Map[String, String] = tagRule
            .split("\n")
            .map { line =>
                val Array(attrName, attrValue) = line.trim.split("=")
                (attrName, attrValue)
            }.toMap
        
        // 3. 返回集合Map
        paramsMap
    }
    
    /**
      * 依据inType判断数据源，封装元数据Meta，加载业务数据
      *
      * @param spark     SparkSession实例对象
      * @param paramsMap 业务数据源参数集合
      */
    def parseMetaToData(spark: SparkSession, paramsMap: Map[String, String]): DataFrame = {
        
        // 1. 从inType获取数据源
        val inType: String = paramsMap("inType")
        
        // 2. 判断数据源，封装Meta，获取业务数据
        val dataframe: DataFrame = inType.toLowerCase match {
            case "hbase" =>
                /*
                 * inType=hbase
                 * zkHosts=hadoop101
                 * zkPort=2181
                 * hbaseTable=tbl_tag_logs
                 * family=detail
                 * selectFieldNames=global_user_id,loc_url,log_time
                 * whereCondition=log_time#day#30
                 */
                
                // 规则数据封装到 HBaseMeta 中
                val hbaseMeta: HBaseMeta = HBaseMeta.getHBaseMeta(paramsMap)
                
                // 依据条件到HBase中获取业务数据
                spark.read
                    .format("hbase")
                    .option("zkHosts", hbaseMeta.zkHosts)
                    .option("zkPort", hbaseMeta.zkPort)
                    .option("hbaseTable", hbaseMeta.hbaseTable)
                    .option("family", hbaseMeta.family)
                    .option("selectFields", hbaseMeta.selectFieldNames)
                    .option("filterConditions", hbaseMeta.filterConditions)
                    .load()
            case "rdbms" =>
                /*
                 * inType=mysql
                 * driver=com.mysql.jdbc.Driver
                 * url=jdbc:mysql://hadoop101:3306/?
                 * useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC
                 * user=root
                 * password=000000
                 * sql=SELECT id,gender FROM tags_dat.tbl_users
                 */
                val mySQLMeta = MySQLMeta.getMySQLMeta(paramsMap)
                
                spark.read.format("jdbc")
                    .option("driver", mySQLMeta.driver)
                    .option("url", mySQLMeta.url)
                    .option("user", mySQLMeta.user)
                    .option("password", mySQLMeta.password)
                    .option("dbtable", mySQLMeta.sql)
                    .load()
            case "hive" =>
                null
            case "hdfs" =>
                null
            case "es" =>
                null
            case _ =>
                // 如果未获取到数据，直接抛出异常
                new RuntimeException("业务标签规则未提供数据源信息，获取不到业务数据，无法计算标签")
                null
        }
        
        // 3. 返回业务数据
        dataframe
    }
    
}

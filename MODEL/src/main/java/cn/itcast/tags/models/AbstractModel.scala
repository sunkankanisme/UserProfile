package cn.itcast.tags.models

import cn.itcast.tags.config.ModelConfig
import cn.itcast.tags.meta.HBaseMeta
import cn.itcast.tags.tools.HBaseTools
import cn.itcast.tags.utils.SparkUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/*
 * 标签基类，标签属性信息
 */
abstract class AbstractModel(modelName: String, modelType: ModelType) extends Logging {

    // 设置Spark应用程序运行的用户：root, 默认情况下为当前系统用户
    System.setProperty("user.name", ModelConfig.FS_USER)
    System.setProperty("HADOOP_USER_NAME", ModelConfig.FS_USER)

    // 变量声明
    var spark: SparkSession = _

    // 1. 初始化：构建SparkSession实例对象
    def init(isHive: Boolean): Unit = {
        spark = SparkUtils.createSparkSession(this.getClass, isHive)
    }

    // 2. 准备标签数据：依据标签ID从MySQL数据库表tbl_basic_tag获取标签数据
    def getTagData(tagId: Long): DataFrame = {
        spark.read.format("jdbc")
            .option("driver", ModelConfig.MYSQL_JDBC_DRIVER)
            .option("url", ModelConfig.MYSQL_JDBC_URL)
            .option("dbtable", ModelConfig.tagTable(tagId))
            .option("user", ModelConfig.MYSQL_JDBC_USERNAME)
            .option("password", ModelConfig.MYSQL_JDBC_PASSWORD)
            .load()
    }

    // 3. 业务数据：依据业务标签规则rule，从数据源获取业务数据
    def getBusinessData(basicTagDF: DataFrame): DataFrame = {
        import basicTagDF.sparkSession.implicits._

        // 获取和解析规则，转换为 Map 集合
        val tagRules = basicTagDF.filter($"level" === 4).head().getAs[String]("rule")

        val tagRuleMap = tagRules.split("\\n").map(line => {
            val Array(k, v) = line.trim.split("=")
            (k, v)
        }).toMap

        // 判断数据源读取业务数据
        var businessDF: DataFrame = null
        if ("hbase".equals(tagRuleMap("inType").toLowerCase())) {
            // 封装标签规则中的数据源信息至 HBaseMeta 对象中
            val hBaseMeta = HBaseMeta.getHBaseMeta(tagRuleMap)
            // 从 HBase 加载数据
            businessDF = HBaseTools.read(spark,
                hBaseMeta.zkHosts,
                hBaseMeta.zkPort,
                hBaseMeta.hbaseTable,
                hBaseMeta.family,
                hBaseMeta.selectFieldNames.split(","))
        } else {
            System.exit(0)
        }

        // businessDF.printSchema()
        // businessDF.show(10, truncate = false)

        businessDF
    }

    // 4. 构建标签：依据业务数据和属性标签数据建立标签
    def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame

    // 5. 保存画像标签数据至HBase表
    def saveTag(modelDF: DataFrame): Unit = {
        // 存储结果到 HBase
        if (modelDF != null) {
            HBaseTools.write(modelDF,
                ModelConfig.PROFILE_TABLE_ZK_HOSTS,
                ModelConfig.PROFILE_TABLE_ZK_PORT,
                ModelConfig.PROFILE_TABLE_NAME,
                ModelConfig.PROFILE_TABLE_FAMILY_DETAIL,
                ModelConfig.PROFILE_TABLE_ROWKEY_COL)
        }
    }

    // 6. 关闭资源：应用结束，关闭会话实例对象
    def close(): Unit = {
        if (null != spark) spark.stop()
    }

    // 规定标签模型执行流程顺序
    def executeModel(tagId: Long, isHive: Boolean = false): Unit = {
        // a. 初始化
        init(isHive)

        try {
            // b. 获取标签数据
            val tagDF: DataFrame = getTagData(tagId)

            // basicTagDF.show()
            tagDF.persist(StorageLevel.MEMORY_AND_DISK)
            tagDF.count()

            // c. 获取业务数据
            val businessDF: DataFrame = getBusinessData(tagDF)
            //businessDF.show()

            // d. 计算标签
            val modelDF: DataFrame = doTag(businessDF, tagDF)
            //modelDF.show()

            // e. 保存标签
            saveTag(modelDF)
            tagDF.unpersist()
        } catch {
            case e: Exception => e.printStackTrace()
        } finally {
            close()
        }
    }
}

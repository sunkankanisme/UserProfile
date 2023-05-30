package cn.itcast.tags.models

import cn.itcast.tags.meta.HBaseMeta
import cn.itcast.tags.tools.HBaseTools
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

trait BasicModel extends Logging {

    // 变量声明
    var spark: SparkSession = _

    // 1. 初始化：构建SparkSession实例对象
    def init(): Unit = {
        System.setProperty("HADOOP_USER_NAME", "root")

        spark = {
            val sparkConf: SparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[Result], classOf[Put]))
                .set("spark.sql.shuffle.partitions", "4")

            SparkSession.builder()
                .config(sparkConf)
                // 启用与Hive集成
                .enableHiveSupport()
                // 设置与Hive集成: 读取Hive元数据MetaStore服务
                .config("hive.metastore.uris", "thrift://hadoop101:9083")
                // 设置数据仓库目录，将 SparkSQL 数据仓库目录与 Hive 的位置一致
                .config("spark.sql.warehouse.dir", "hdfs://hadoop101/user/hive/warehouse")
                .getOrCreate()
        }
    }

    // 2. 准备标签数据：依据标签ID从MySQL数据库表tbl_basic_tag获取标签数据
    def getTagData(tagId: Long): DataFrame = {
        val tagTable = s"(select id,name,rule,level from tbl_basic_tag where id=$tagId or pid=$tagId) t1"
        spark.read.format("jdbc")
            .option("driver", "com.mysql.jdbc.Driver")
            .option("url", "jdbc:mysql://hadoop101:3306/profile_tags?useSSL=false&useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC")
            .option("dbtable", tagTable)
            .option("user", "root")
            .option("password", "000000")
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

    // 5. 5. 保存画像标签数据至HBase表
    def saveTag(modelDF: DataFrame): Unit = {
        // 存储结果到 HBase
        HBaseTools.write(modelDF, "hadoop101", "2181", "tbl_profile", "detail", "userId")
    }

    // 6. 关闭资源：应用结束，关闭会话实例对象
    def close(): Unit = {
        if (spark != null) {
            spark.stop()
        }
    }

    // 规定标签模型执行流程顺序
    def executeModel(tagId: Long): Unit = {
        // a. 初始化
        init()

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

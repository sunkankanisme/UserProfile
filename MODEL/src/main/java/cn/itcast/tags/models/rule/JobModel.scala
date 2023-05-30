package cn.itcast.tags.models.rule

import cn.itcast.tags.meta.HBaseMeta
import cn.itcast.tags.tools.HBaseTools
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
 * 规则匹配标签-职业
 */
object JobModel {

    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "root")

        val spark: SparkSession = {
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

        import org.apache.spark.sql.functions._
        import spark.implicits._

        // 读取标签数据
        val tagTable = "(select id,name,rule,level from tbl_basic_tag where id=321 or pid=321) t1"
        val basicTagDF = spark.read.format("jdbc")
            .option("driver", "com.mysql.jdbc.Driver")
            .option("url", "jdbc:mysql://hadoop101:3306/profile_tags?useSSL=false&useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC")
            .option("dbtable", tagTable)
            .option("user", "root")
            .option("password", "000000")
            .load()

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

        // 取出标签规则，转换为 Map 集合
        val tagRule: Map[String, String] = basicTagDF
            .filter($"level" === 5)
            .select($"rule", $"name")
            .as[(String, String)]
            .rdd
            .collectAsMap()
            .toMap

        val tagRuleBroadcast = spark.sparkContext.broadcast(tagRule)

        // 业务数据和标签规则关联，使用 UDF 函数
        val jobUDF: UserDefinedFunction = udf(
            (job: String) => {
                tagRuleBroadcast.value(job)
            }
        )

        val modelDF = businessDF.select($"id".as("userId"), jobUDF($"job").as("job"))
        // modelDF.printSchema()
        modelDF.show(10, truncate = false)

        // 存储结果到 HBase
        HBaseTools.write(modelDF, "hadoop101", "2181", "tbl_profile", "detail", "userId")

        spark.stop()
    }

}

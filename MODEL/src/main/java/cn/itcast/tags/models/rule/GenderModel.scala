package cn.itcast.tags.models.rule

import cn.itcast.tags.meta.HBaseMeta
import cn.itcast.tags.tools.HBaseTools
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 规则匹配标签，用户性别标签模型
 */
object GenderModel {

    def main(args: Array[String]): Unit = {

        // 创建SparkSession实例对象
        val spark: SparkSession = {
            val sparkConf = new SparkConf()
                .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
                .setMaster("local[*]")
                .set("spark.sql.shuffle.partitions", "4")
                // 由于从HBase表读写数据，设置序列化
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[Result], classOf[Put]))

            SparkSession.builder()
                .config(sparkConf)
                .getOrCreate()
        }

        import org.apache.spark.sql.functions._
        import spark.implicits._

        // 从MySQL数据库读取标签数据（基础标签表：tbl_basic_tag），依据业务标签ID读取
        val tagTable = "(select id,name,rule,level from tbl_basic_tag where id=318 or pid=318) t1"
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

        // Map(inType -> hbase,
        //     zkHosts -> bigdata-cdh01.itcast.cn,
        //     zkPort -> 2181,
        //     hbaseTable -> tbl_tag_users,
        //     selectFieldNames -> id,gender,
        //     family -> detail)
        println(tagRuleMap)

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

        // businessDF.show(10, truncate = false)

        // 取出标签规则
        val tagRuleDF = basicTagDF.filter($"level" === 5).select($"rule", $"name")

        // 业务数据和标签规则关联
        val modelDF = businessDF.join(tagRuleDF, $"gender" === $"rule", "inner")
            .select($"id".as("userId"), $"name".as("gender"))

        modelDF.printSchema()
        // modelDF.show(10, truncate = false)

        // 依据业务标签规则获取业务数据，比如到HBase数据库读取表的数据
        // 业务数据和属性标签结合，构建标签：规则匹配型标签 -> rule match
        // 将标签数据存储到HBase表中：用户画像标签表 -> tbl_profile [create 'tbl_profile', 'detail']
        HBaseTools.write(modelDF, "hadoop101", "2181", "tbl_profile", "detail", "userId")

        // 应用结束，关闭资源
        spark.stop()
    }

}

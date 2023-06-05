package cn.itcast.tags.test.hbase.filter

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 测试自定义外部数据源实现从HBase表读写数据接口
 */
object HBaseSQLFilterTest {

    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "root")

        val spark = SparkSession.builder()
            .appName(this.getClass.getSimpleName.stripSuffix("$"))
            .master("local[*]")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()

        // 读取数据
        val usersDF: DataFrame = spark.read
            // 指定包的位置[cn.itcast.tags.spark.hbase]，注册数据源之后可以使用 [hbase] 简短名称
            .format("hbase")
            .option("zkHosts", "hadoop101")
            .option("zkPort", "2181")
            .option("hbaseTable", "tbl_tag_users")
            .option("family", "detail")
            .option("selectFields", "id,gender")
            // 设置过滤条件进行测试
            .option("filterConditions", "gender[eq]2")
            .load()

        usersDF.printSchema()
        usersDF.cache()
        usersDF.show(10, truncate = false)

        // 保存数据
        println(usersDF.count())

        spark.stop()
    }

}

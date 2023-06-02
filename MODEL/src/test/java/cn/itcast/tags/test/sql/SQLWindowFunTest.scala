package cn.itcast.tags.test.sql

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
 * 演示 DSL 风格中如何使用窗口函数
 * 注意：此类未测试
 */
object SQLWindowFunTest {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .appName(this.getClass.getSimpleName.stripSuffix("$"))
            .master("local[4]")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            // 设置Shuffle分区数目
            .config("spark.sql.shuffle.partitions", "4")
            // 设置与Hive集成: 读取Hive元数据MetaStore服务
            .config("hive.metastore.uris", "thrift://hadoop101: 9083")
            // 设置数据仓库目录
            .config("spark.sql.warehouse.dir", "hdfs://hadoop101: 8020/user/hive/warehouse")
            .enableHiveSupport()
            .getOrCreate()

        import org.apache.spark.sql.functions._
        import spark.implicits._

        // 1. 从Hive表中读取雇员表数据[db_hive.emp]
        val empDF: DataFrame = spark.read
            .table("db_hive.emp")
            .select($"empno", $"ename", $"sal", $"deptno")

        // 2. 需求：各个部门工资最高的人员信息，使用开窗函数row_number
        // 方式一：使用SQL实现
        // a. 注册DataFrame为临时视图
        empDF.createOrReplaceTempView("view_tmp_emp")

        // b. 编写SQL
        spark.sql(
            """
              | WITH tmp AS (
              |     SELECT
              |      *,
              |      ROW_NUMBER() OVER(PARTITION BY deptno ORDER BY sal DESC) AS rnk
              |     FROM
              |      view_tmp_emp
              | )
              | SELECT
              |      t.empno, t.ename, t.sal, t.deptno
              | FROM tmp t
              | WHERE t.rnk = 1
              | """.stripMargin)
            .show()

        // 方式二：使用DSL编程实现
        empDF.
            // 使用函数，增加一列
            withColumn(
                "rnk",
                row_number().over(
                    Window.partitionBy($"deptno").orderBy($"sal".desc)
                )
            )
            .withColumn(
                "rk",
                rank().over(Window.partitionBy($"deptno").orderBy($"sal".desc))
            )
            // 获取rnk=1
            .where($"rnk" === 1)
            .select($"empno", $"ename", $"sal", $"deptno")
            .show()

        spark.stop()
    }

}

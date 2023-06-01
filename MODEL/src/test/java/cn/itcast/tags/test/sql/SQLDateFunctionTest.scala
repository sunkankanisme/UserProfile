package cn.itcast.tags.test.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/*
 * sql 日期函数测试代码
 */
object SQLDateFunctionTest {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .appName(this.getClass.getSimpleName.stripSuffix("$"))
            .master("local[4]")
            .getOrCreate()

        import org.apache.spark.sql.functions._
        import spark.implicits._

        // 模拟数据
        val dataframe: DataFrame = Seq(
            "1659715200"
        ).toDF("finishtime")

        // 将 long 类型的数据转化为日期时间类型
        val df = dataframe.select(
            from_unixtime($"finishtime").as("finish_time"),
            current_timestamp().as("now_time")
        )
        df.show()

        // 计算天数
        val df2 = df.select(datediff($"now_time", $"finish_time").as("diff_time"))
        df2.show()

        // 使用日期进行处理
        dataframe.select(
            from_unixtime($"finishtime", "yyyy-MM-dd").as("finish_date"),
            current_date().as("now_date")
        ).select(datediff($"now_date", $"finish_date").as("diff_time"))
            .show(false)

        spark.stop
    }

}

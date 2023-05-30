package cn.itcast.tags.test.config

import com.typesafe.config.{ConfigFactory, ConfigValue}

import java.util
import java.util.Map

object SparkConfigTest {

    def main(args: Array[String]): Unit = {
        // 使用ConfigFactory加载spark.conf
        val config = ConfigFactory.load("spark.conf")

        // 获取加载配置信息
        val entrySet: util.Set[Map.Entry[String, ConfigValue]] = config.entrySet()

        // 遍历
        import scala.collection.JavaConverters._
        for (entry <- entrySet.asScala) {
            // 获取属性来源的文件名称
            val resource = entry.getValue.origin().resource()

            // 排除系统属性，仅需要 “spark.conf” 文件的配置
            if ("spark.conf".equals(resource)) {
                println(entry.getKey + ": " + entry.getValue.unwrapped().toString)
            }
        }
    }

}

package cn.itcast.tags.utils

import cn.itcast.tags.config.ModelConfig
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkUtils {

    /**
     * 加载 Spark Application 默认配置文件，设置到SparkConf中
     *
     * @param resource 资源配置文件名称
     * @return SparkConf对象
     */
    def loadConf(resource: String): SparkConf = {
        // 1. 创建SparkConf 对象
        val sparkConf = new SparkConf()

        // 2. 使用ConfigFactory加载配置文件
        val config: Config = ConfigFactory.load(resource)

        // 3. 获取加载配置信息
        val entrySet = config.entrySet()

        // 4. 循环遍历设置属性值到SparkConf中
        import scala.collection.JavaConverters._
        entrySet.asScala.foreach { entry =>
            // 获取属性来源的文件名称
            val resourceName = entry.getValue.origin().resource()
            if (resource.equals(resourceName)) {
                sparkConf.set(entry.getKey,
                    entry.getValue.unwrapped().toString)
            }
        }

        // 5. 返回SparkConf对象
        sparkConf
    }

    /**
     * 构建SparkSession实例对象，如果是本地模式，设置master
     */
    def createSparkSession(clazz: Class[_], isHive: Boolean = false): SparkSession = {
        // 构建SparkConf对象
        val sparkConf: SparkConf = loadConf(resource = "spark.properties")

        // 判断应用是否是本地模式运行，如果是设置
        if (ModelConfig.APP_IS_LOCAL) {
            sparkConf.setMaster(ModelConfig.APP_SPARK_MASTER)
        }

        // 创建SparkSession.Builder对象
        var builder: SparkSession.Builder = SparkSession.builder()
            .appName(clazz.getSimpleName.stripSuffix("$"))
            .config(sparkConf)

        // 判断应用是否集成Hive，如果集成，设置Hive MetaStore地址
        //      如果在 config.properties 中设置集成 Hive，表示所有 SparkApplication 都集成 Hive
        //      否则判断 isHive，表示针对某个具体应用是否集成 Hive
        if (ModelConfig.APP_IS_HIVE || isHive) {
            builder = builder
                .config("hive.metastore.uris", ModelConfig.APP_HIVE_META_STORE_URL)
                .enableHiveSupport()
        }

        // 获取SparkSession对象
        val session = builder.getOrCreate()

        // 返回
        session
    }

}

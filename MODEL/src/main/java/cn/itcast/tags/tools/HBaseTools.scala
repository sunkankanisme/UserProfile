package cn.itcast.tags.tools

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * HBase 工具类
 */
object HBaseTools {

    /**
     * 依据指定表名称、列簇及列名称从HBase表中读取数据
     *
     * @param spark  SparkSession 实例对象
     * @param zks    Zookerper 集群地址
     * @param port   Zookeeper端口号
     * @param table  HBase表的名称
     * @param family 列簇名称
     * @param fields 列名称
     * @return
     */
    def read(spark: SparkSession, zks: String, port: String, table: String, family: String, fields: Seq[String]): DataFrame = {
        val sc = spark.sparkContext

        // HBase 配置信息
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", zks)
        conf.set("hbase.zookeeper.property.clientPort", port)
        conf.set("zookeeper.znode.parent", "/hbase")
        conf.set(TableInputFormat.INPUT_TABLE, table)

        // 设置读取的列簇和列名称
        val scan = new Scan()
        val cfBytes: Array[Byte] = Bytes.toBytes(family)
        scan.addFamily(cfBytes)
        for (elem <- fields) {
            scan.addColumn(cfBytes, Bytes.toBytes(elem))
        }
        // Bytes.toString(Base64.getEncoder().encode(ProtobufUtil.toScan(scan).toByteArray()));
        conf.set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(scan))

        // 加载数据
        val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
            conf,
            classOf[TableInputFormat],
            classOf[ImmutableBytesWritable],
            classOf[Result]
        )

        // 转换为 rdd[row]
        val rowRdd = hbaseRDD.map {
            case (_, result) =>
                // 获取指定的字段值并且转换为字符串
                val values: Seq[String] = fields.map(f => new String(result.getValue(cfBytes, Bytes.toBytes(f))))

                // 转换为 Row 类型
                Row.fromSeq(values)
        }

        // 定义 rdd 对应的 schema
        val schema: StructType = StructType(
            fields.map(f => StructField(f, StringType, nullable = true))
        )

        // 将 rdd 转为 df
        spark.createDataFrame(rowRdd, schema)
    }

    /**
     * 将DataFrame数据保存到HBase表中
     *
     * @param dataframe    数据集DataFrame
     * @param zks          Zk地址
     * @param port         端口号
     * @param table        表的名称
     * @param family       列簇名称
     * @param rowKeyColumn RowKey字段名称
     */
    def write(dataframe: DataFrame, zks: String, port: String, table: String, family: String, rowKeyColumn: String): Unit = {
        // HBase 配置信息
        val conf: Configuration = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", zks)
        conf.set("hbase.zookeeper.property.clientPort", port)
        conf.set("zookeeper.znode.parent", "/hbase")
        conf.set(TableOutputFormat.OUTPUT_TABLE, table)

        // 列簇
        val cfBytes = Bytes.toBytes(family)

        // 列信息
        val columns: Array[String] = dataframe.columns

        // 将 df 转换为 rdd[(RowKey, Put)]
        val putRdd = dataframe.rdd.map(row => {
            // 获取 rowKey
            val rowKey: Array[Byte] = Bytes.toBytes(row.getAs[String](rowKeyColumn))

            // 构建 Put 对象
            val put = new Put(rowKey)
            columns.foreach(col => {
                put.addColumn(
                    cfBytes,
                    Bytes.toBytes(col),
                    Bytes.toBytes(row.getAs[String](col)))
            })

            // 返回二元组
            (new ImmutableBytesWritable(put.getRow), put)
        })

        // 保存 rdd 数据到 HBase 表
        putRdd.saveAsNewAPIHadoopFile(
            s"datas/hbase/output-${System.nanoTime()}",
            classOf[ImmutableBytesWritable],
            classOf[Put],
            classOf[TableOutputFormat[ImmutableBytesWritable]],
            conf
        )
    }

}

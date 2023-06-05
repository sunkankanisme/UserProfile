package cn.itcast.tags.spark.sql

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.filter.{FilterList, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
 * 定义外部数据源：从 HBase 表加载数据和保存数据值 HBase 表
 */
class HBaseRelation(context: SQLContext,
                    params: Map[String, String],
                    userSchema: StructType
                   ) extends BaseRelation with TableScan with InsertableRelation with Serializable {

    // 连接HBase数据库的属性名称
    val HBASE_ZK_QUORUM_KEY: String = "hbase.zookeeper.quorum"
    val HBASE_ZK_QUORUM_VALUE: String = "zkHosts"
    val HBASE_ZK_PORT_KEY: String = "hbase.zookeeper.property.clientPort"
    val HBASE_ZK_PORT_VALUE: String = "zkPort"
    val HBASE_TABLE: String = "hbaseTable"
    val HBASE_TABLE_FAMILY: String = "family"
    val SPERATOR: String = ","
    val HBASE_TABLE_SELECT_FIELDS: String = "selectFields"
    val HBASE_TABLE_ROWKEY_NAME: String = "rowKeyColumn"

    /*
     * SparkSQL 加载和保存数据的入口
     */
    override def sqlContext: SQLContext = context

    /*
     * 在 SparkSQL 中数据封装在df或ds中的 schema
     */
    override def schema: StructType = userSchema

    /*
     * 从数据源加载数据，封装至 RDD 中，每条数据在 Row 中，结合 Schema 转换为 DataFrame
     */
    override def buildScan(): RDD[Row] = {
        // HBase 配置信息
        val conf = HBaseConfiguration.create()
        conf.set(HBASE_ZK_QUORUM_KEY, params(HBASE_ZK_QUORUM_VALUE))
        conf.set(HBASE_ZK_PORT_KEY, params(HBASE_ZK_PORT_VALUE))
        conf.set(TableInputFormat.INPUT_TABLE, params(HBASE_TABLE))

        // 设置读取的列簇和列名称
        val scan = new Scan()
        val cfBytes: Array[Byte] = Bytes.toBytes(params(HBASE_TABLE_FAMILY))
        scan.addFamily(cfBytes)
        val fields = params(HBASE_TABLE_SELECT_FIELDS).split(SPERATOR)
        for (elem <- fields) {
            scan.addColumn(cfBytes, Bytes.toBytes(elem))
        }

        /*
         * 设置过滤条件的配置
         */
        val filterConditions = params.getOrElse("filterConditions", null)

        // 如果传入了过滤条件，则创建过滤器
        // 多个过滤条件使用逗号分割，例：modified[GE]20230601,modified[LE]20230605
        val filterList: FilterList = new FilterList()
        if (filterConditions != null) {
            val strings = filterConditions.split(",")

            // 解析每个条件构建成单值过滤器
            strings.foreach(filterCondition => {
                // 解析过滤条件封装到样例类中，modified[GE]20230601
                val condition: Condition = Condition.parseCondition(filterCondition)

                // 创建过滤条件并添加到 filterList 中
                val filter: SingleColumnValueFilter = new SingleColumnValueFilter(
                    cfBytes,
                    Bytes.toBytes(condition.field),
                    condition.compare,
                    Bytes.toBytes(condition.value)
                )

                filterList.addFilter(filter)

                // 注意：必须要获取需要过滤列的值
                scan.addColumn(cfBytes, Bytes.toBytes(condition.field))
            })
        }

        // 将过滤器添加到扫描器
        scan.setFilter(filterList)

        // Bytes.toString(Base64.getEncoder().encode(ProtobufUtil.toScan(scan).toByteArray()));
        conf.set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(scan))

        // 加载数据
        val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sqlContext.sparkContext.newAPIHadoopRDD(
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

        // 返回 rdd
        rowRdd
    }

    /*
     * 将 df 保存到数据源
     */
    override def insert(data: DataFrame, overwrite: Boolean): Unit = {
        println(">>> HBaseRelation#insert")

        // HBase 配置信息
        val conf: Configuration = HBaseConfiguration.create()
        conf.set(HBASE_ZK_QUORUM_KEY, params(HBASE_ZK_QUORUM_VALUE))
        conf.set(HBASE_ZK_PORT_KEY, params(HBASE_ZK_PORT_VALUE))
        conf.set(TableOutputFormat.OUTPUT_TABLE, params(HBASE_TABLE))

        // 列簇
        val cfBytes = Bytes.toBytes(params(HBASE_TABLE_FAMILY))

        // 列信息
        val columns: Array[String] = data.columns

        // 将 df 转换为 rdd[(RowKey, Put)]
        val putRdd = data.rdd.map(row => {
            // 获取 rowKey
            val rowKey: Array[Byte] = Bytes.toBytes(row.getAs[String](params(HBASE_TABLE_ROWKEY_NAME)))

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

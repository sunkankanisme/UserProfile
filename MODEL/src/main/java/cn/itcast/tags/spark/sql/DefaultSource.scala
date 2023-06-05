package cn.itcast.tags.spark.sql

import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
 * 默认数据源提供 relation 对象，为加载和保存数据提供 relation 对象
 */
class DefaultSource extends RelationProvider with CreatableRelationProvider with DataSourceRegister {

    /*
     * 注册数据源短名称，加载数据时不需要再写包名
     * - 注意类名需要为 DefaultSource
     * - 还需要在 META-INF/services 目录下进行配置
     */
    override def shortName(): String = "hbase"

    val SPERATOR: String = ","
    val HBASE_TABLE_SELECT_FIELDS: String = "selectFields"

    /*
     * 从数据源读取数据，创建 Relation 对象（实现 BaseRelation，TableScan）
     * parameters：通过 .option() 设置的参数
     */
    override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
        // 自定义 schema 信息，从传入的参数中生成
        val fields = parameters(HBASE_TABLE_SELECT_FIELDS).split(SPERATOR)
        val userSchema: StructType = new StructType(
            fields.map(field => {
                StructField(field, StringType, nullable = true)
            })
        )

        // 创建 HBaseRelation 对象
        val baseRelation = new HBaseRelation(sqlContext, parameters, userSchema)

        // 返回 BaseRelation 对象
        baseRelation
    }

    /*
     * 将数据集保存到数据源时，创建 Relation 对象（实现 BaseRelation，InsertableRelation）
     * parameters：通过 .option() 设置的参数
     */
    override def createRelation(sqlContext: SQLContext,
                                mode: SaveMode,
                                parameters: Map[String, String],
                                data: DataFrame): BaseRelation = {
        // 创建 HBaseRelation 对象
        val relation = new HBaseRelation(sqlContext, parameters, data.schema)

        // 保存数据
        relation.insert(data, overwrite = true)

        // 返回
        relation
    }

}

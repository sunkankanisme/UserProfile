package cn.itcast.tags.meta

import cn.itcast.tags.utils.DateUtils

case class HBaseMeta(zkHosts: String,
                     zkPort: String,
                     hbaseTable: String,
                     family: String,
                     selectFieldNames: String,
                     filterConditions: String)

object HBaseMeta {

    /**
     * 将Map集合数据解析到HBaseMeta中
     *
     * @param ruleMap map集合
     * @return
     */
    def getHBaseMeta(ruleMap: Map[String, String]): HBaseMeta = {
        /*
         * 从 mysql 的 rule 字段中加载 whereCondition[modified#day#30] 解析为动态的 filterConditions[modified[ge]20230501]
         */
        val whereCondition = ruleMap.getOrElse("whereCondition", null)
        val filterConditions: String = if (null != whereCondition) {
            val Array(field, unit, amount) = whereCondition.split("#")

            // 解析日期
            val nowDate = DateUtils.getNow()
            val yesterday = DateUtils.dateCalculate(nowDate, -1)
            val agoDate = unit match {
                case "day" =>
                    DateUtils.dateCalculate(nowDate, -(1 * amount.toInt))
                case "month" =>
                    DateUtils.dateCalculate(nowDate, -(30 * amount.toInt))
                case "year" =>
                    DateUtils.dateCalculate(nowDate, -(365 * amount.toInt))
            }

            // 封装字符串
            s"$field[ge]$agoDate,$field[le]$yesterday"
        } else null

        // 实际开发中，应该先判断各个字段是否有值，没有值直接给出提示，终止程序运行，此处省略
        HBaseMeta(
            ruleMap("zkHosts"),
            ruleMap("zkPort"),
            ruleMap("hbaseTable"),
            ruleMap("family"),
            ruleMap("selectFieldNames"),
            filterConditions
        )
    }

}

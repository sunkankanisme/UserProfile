package cn.itcast.tags.spark.sql

import org.apache.hadoop.hbase.CompareOperator
import scala.util.matching.Regex

/**
 * 封装FILTER CLAUSE语句至Condition对象中
 *
 * @param field   字段名称
 * @param compare 字段比较操作，eq、ne、gt、lt、ge和le
 * @param value   字段比较的值
 */
case class Condition(field: String, compare: CompareOperator, value: String)

/*
 * 在样例类的伴生对象中提供解析的方法
 */
object Condition {

    /*
     * 正则表达式
     * "."：匹配除了换行符以外的任何字符
     * "*"(贪婪)：重复零次或更多
     * "?"(占有)：重复零次或一次
     * "()"：标记一个子表达式的开始和结束位置
     */
    private val FULL_REGEX: Regex = "(.*?)\\[(.*?)\\](.*+)".r

    /**
     * 解析Filter Clause，封装到Condition类中
     *
     * @param filterCondition 封装where语句，格式为：modified[GE]20190601
     * @return Condition对象
     */
    def parseCondition(filterCondition: String): Condition = {
        // 1. 使用正则表达式，或者分割字符串
        val optionMatch: Option[Regex.Match] = FULL_REGEX.findFirstMatchIn(filterCondition)

        // 2. 获取匹配Regex.Match对象
        val matchValue: Regex.Match = optionMatch.get

        /*
         * 3. 获取比较操作符，转换为CompareOp对象
         *
         * 获取表达式，不区分大小写
         * 例如："modified[GE]20190601",提取的内容为GE
         * EQ = EQUAL等于
         * NE = NOT EQUAL不等于
         * GT = GREATER THAN大于
         * LT = LESS THAN小于
         * GE = GREATER THAN OR EQUAL 大于等于
         * LE = LESS THAN OR EQUAL 小于等于
         */
        val compare: CompareOperator = matchValue.group(2).toLowerCase match {
            case "eq" => CompareOperator.EQUAL
            case "ne" => CompareOperator.NOT_EQUAL
            case "gt" => CompareOperator.GREATER
            case "lt" => CompareOperator.LESS
            case "ge" => CompareOperator.GREATER_OR_EQUAL
            case "le" => CompareOperator.LESS_OR_EQUAL
        }

        // 4. 从Match中获取对应的值
        Condition(
            matchValue.group(1),
            compare,
            matchValue.group(3)
        )
    }

}
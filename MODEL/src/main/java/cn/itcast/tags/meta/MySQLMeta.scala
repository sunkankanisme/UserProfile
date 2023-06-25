package cn.itcast.tags.meta

/*
 * RDBMS 关系型数据库元数据解析存储
 *
 * inType=mysql
 * driver=com.mysql.jdbc.Driver
 * url=jdbc:mysql://hadoop101:3306/?
 * useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC
 * user=root
 * password=000000
 * sql=SELECT id,gender FROM tags_dat.tbl_users
 */
case class MySQLMeta(driver: String,
                     url: String,
                     user: String,
                     password: String,
                     sql: String)

object MySQLMeta {
    
    /**
      * 将 Map 集合数据解析到 RdbmsMeta 中
      *
      * @param ruleMap map集合
      */
    def getMySQLMeta(ruleMap: Map[String, String]): MySQLMeta = {
        // 获取SQL语句，赋以别名
        val sqlStr: String = s"( ${ruleMap("sql")} ) AS tmp"
        
        // 构建RdbmsMeta对象
        MySQLMeta(
            ruleMap("driver"),
            ruleMap("url"),
            ruleMap("user"),
            ruleMap("password"),
            sqlStr
        )
    }
    
}
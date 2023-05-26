# 主要功能

- 代码适配 Spark3 & HBase2 版本

# 注意事项

- 类路径和代码风格有一定的改变，功能与原版一致，建议直接使用 `ctrl + N` 直接搜索你需要查看的类
- 环境基于开源 hadoop 集群，主节点为 `hadoop101` 根据自己的环境修改，资源路径 `resources` 下 xml 文件也记得替换
- 代码仅供参考，有 bug 可以直接联系我，大部分都是测试通过的

# 导航

- P33，Spark 读取 Hive 文件写入 HBase 代码 `cn.itcast.tags.etl.hfile.HBaseBulkLoader`
- WEB 项目，移除了对 tags-up 的依赖 `cn.itcast.tags.web.WebApplication`，资源位置 `application.properties` 注意修改数据源配置


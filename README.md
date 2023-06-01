# 主要功能

- 代码适配 Hadoop3.1.3、Spark3.3.2、HBase2.4.11 版本

# 注意事项

- 类路径和代码风格有一定的改变，功能与原版一致，建议直接使用 `ctrl + N` 直接搜索你需要查看的类
- 环境基于开源 hadoop 集群，节点为 `hadoop[101 - 103]` 代码中需要根据自己的环境修改，资源路径 `resources` 下 xml 文件和
  SpringBoot 用到的 `application.properties` 也记得修改
- 代码仅供参考，有 bug 可以直接联系我，大部分都是测试通过的（关于 Oozie 相关的都没有测试 -_-！）

# 导航

- P33 - Spark 读取 Hive 文件写入 HBase 代码：`cn.itcast.tags.etl.hfile.HBaseBulkLoader`
- P48 - WEB 项目测试：`cn.itcast.tags.web.WebApplication`
- P54 - 读写 HBase 测试：`cn.itcast.tags.test.hbase.read.HBaseReadTest`，`cn.itcast.tags.test.hbase.write.HBaseWriteTest`

# 不同

- WEB 项目
    - 移除了对 tags-up 的依赖
    - 移除任务调度功能，点击开始或结束时仅打印日志（现在的环境使用 DolphinScheduler 进行调度，后续计划修改为使用
      DolphinScheduler API 进行调度）
    - 移除了创建标签时上传 jar 到 Hdfs 的功能（因为现在移除了任务调度功能）
- ETL 项目
    - 优化 MockOrders 逻辑，计算 finish_time 字段以当前时间为基准

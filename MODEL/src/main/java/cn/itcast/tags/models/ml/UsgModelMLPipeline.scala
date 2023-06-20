package cn.itcast.tags.models.ml

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer, VectorIndexerModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

/*
 * USG 模型开发，使用 Pipeline 决策树 算法
 */
class UsgModelMLPipeline extends AbstractModel("用户购物性别", ModelType.ML) {
    override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
        import businessDF.sparkSession.implicits._
        
        // +--------------------+--------+-----------+
        // |            cordersn| ogcolor|producttype|
        // +--------------------+--------+-----------+
        // |jd_14091818005983607|     白色|       烤箱|
        // |jd_14091317283357943|   香槟金|       冰吧|
        // |jd_14092012560709235|  香槟金色|     净水机|
        // +--------------------+--------+-----------+
        // businessDF.show(3)
        
        // +---+----+----+-----+
        // | id|name|rule|level|
        // +---+----+----+-----+
        // |379|  男|   0|    5|
        // |380|  女|   1|    5|
        // |381|中性|  -1|    5|
        // +---+----+----+-----+
        // tagDF.filter("level = 5").show(10)
        
        /*
         * 1 加载三张维度表
         */
        
        // 1.1 获取订单表数据tbl_orders，与订单商品表数据关联获取会员ID
        val ordersDF: DataFrame = spark.read
            .format("hbase")
            .option("zkHosts", "hadoop101")
            .option("zkPort", "2181")
            .option("hbaseTable", "tbl_tag_orders")
            .option("family", "detail")
            .option("selectFields", "memberid,ordersn")
            .load()
        
        // +--------+--------------------+
        // |memberid|ordersn             |
        // +--------+--------------------+
        // |265     |gome_792756751164275|
        // |208     |jd_14090106121770839|
        // |673     |jd_14090112394810659|
        // +--------+--------------------+
        // ordersDF.show(3, truncate = false)
        
        // 1.2 加载字典表数据 - 颜色
        val colorsDF: DataFrame = {
            spark.read
                .format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", "jdbc:mysql://hadoop101:3306/?useSSL=false&useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC")
                .option("dbtable", "profile_tags.tbl_dim_colors")
                .option("user", "root")
                .option("password", "000000")
                .load()
        }
        
        // +---+----------+
        // |id |color_name|
        // +---+----------+
        // |1  |香槟金色   |
        // |2  |黑色      |
        // |3  |白色      |
        // +---+----------+
        // colorsDF.show(3, truncate = false)
        
        // 1.3 加载字典表数据 - 商品
        val productsDF: DataFrame = {
            spark.read
                .format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", "jdbc:mysql://hadoop101:3306/?useSSL=false&useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC")
                .option("dbtable", "profile_tags.tbl_dim_products")
                .option("user", "root")
                .option("password", "000000")
                .load()
        }
        
        // +---+--------------+
        // |id |product_name  |
        // +---+--------------+
        // |1  |4K电视         |
        // |2  |Haier/海尔冰箱  |
        // |3  |LED电视        |
        // +---+--------------+
        // productsDF.show(3, truncate = false)
        
        
        /*
         * 2 关联维度数据
         */
        // 2.1 构建颜色WHEN语句
        val colorColumn: Column = {
            // 声明变量
            var colorCol: Column = null
            
            colorsDF.as[(Int, String)].rdd.collectAsMap().foreach {
                case (colorId, colorName) =>
                    if (null == colorCol) {
                        colorCol = when($"ogcolor".equalTo(colorName), colorId)
                    } else {
                        colorCol = colorCol.when($"ogcolor".equalTo(colorName), colorId)
                    }
            }
            
            colorCol.otherwise(0).as("color")
        }
        
        // 2.2 构建产品WHEN语句
        val productColumn: Column = {
            // 声明变量
            var productCol: Column = null
            productsDF.as[(Int, String)].rdd.collectAsMap().foreach {
                case (productId, productName) =>
                    if (null == productCol) {
                        productCol = when($"producttype".equalTo(productName), productId)
                    } else {
                        productCol = productCol.when($"producttype".equalTo(productName), productId)
                    }
            }
            
            productCol.otherwise(0).as("product")
        }
        
        /*
         * 2.3 数据标注
         * - 根据运营的前期的统计分析， 对数据进行性别类别的标注
         * - 目标是根据运营规则标注的部分数据，进行决策树模型的训练，以后来了新的数据，模型就可以判断出购物性别是男还是女
         */
        val labelColumn: Column = {
            when($"ogcolor".equalTo("樱花粉")
                .or($"ogcolor".equalTo("白色"))
                .or($"ogcolor".equalTo("香槟色"))
                .or($"ogcolor".equalTo("香槟金"))
                .or($"productType".equalTo("料理机"))
                .or($"productType".equalTo("挂烫机"))
                // 女
                .or($"productType".equalTo("吸尘器/除螨仪")), 1)
                // 男
                .otherwise(0)
                // 决策树预测label
                .alias("label")
        }
        
        val goodsDF = businessDF
            .join(ordersDF, $"cordersn" === $"ordersn")
            .select($"memberid".as("userId"), colorColumn, productColumn, labelColumn)
        goodsDF.cache()
        
        // +------+-----+-------+-----+
        // |userId|color|product|label|
        // +------+-----+-------+-----+
        // |442   |11   |2      |0    |
        // |442   |1    |8      |0    |
        // |530   |15   |0      |0    |
        // +------+-----+-------+-----+
        // goodsDF.show(3, truncate = false)
        
        /*
         * TODO 构建算法模型，并使用模型预测 USG
         *  使用数据集构建管道模型
         */
        val pipelineModel = trainPipelineModel(goodsDF)
        val predictionDF = pipelineModel.transform(goodsDF)
        
        println("======================== goodsDF")
        // +------+-----+-------+-----+
        // |userId|color|product|label|
        // +------+-----+-------+-----+
        // |442   |11   |2      |0    |
        // |442   |1    |8      |0    |
        // |530   |15   |0      |0    |
        // |768   |19   |6      |0    |
        // |768   |15   |28     |0    |
        // |857   |18   |8      |0    |
        // |16    |2    |0      |0    |
        // |84    |15   |9      |0    |
        // |58    |4    |0      |0    |
        // |506   |10   |27     |0    |
        // +------+-----+-------+-----+
        goodsDF.show(10, truncate = false)
        println("======================== predictionDF")
        // +------+-----+-------+-----+------------+-----------+-------------+-----------+----------+
        // |userId|color|product|label|raw_features|features   |rawPrediction|probability|prediction|
        // +------+-----+-------+-----+------------+-----------+-------------+-----------+----------+
        // |442   |11   |2      |0    |[11.0,2.0]  |[10.0,2.0] |[74124.0,0.0]|[1.0,0.0]  |0.0       |
        // |442   |1    |8      |0    |[1.0,8.0]   |[0.0,8.0]  |[74124.0,0.0]|[1.0,0.0]  |0.0       |
        // |530   |15   |0      |0    |[15.0,0.0]  |[14.0,0.0] |[74124.0,0.0]|[1.0,0.0]  |0.0       |
        // |768   |19   |6      |0    |[19.0,6.0]  |[18.0,6.0] |[74124.0,0.0]|[1.0,0.0]  |0.0       |
        // |768   |15   |28     |0    |[15.0,28.0] |[14.0,27.0]|[74124.0,0.0]|[1.0,0.0]  |0.0       |
        // |857   |18   |8      |0    |[18.0,8.0]  |[17.0,8.0] |[74124.0,0.0]|[1.0,0.0]  |0.0       |
        // |16    |2    |0      |0    |[2.0,0.0]   |[1.0,0.0]  |[74124.0,0.0]|[1.0,0.0]  |0.0       |
        // |84    |15   |9      |0    |[15.0,9.0]  |[14.0,9.0] |[74124.0,0.0]|[1.0,0.0]  |0.0       |
        // |58    |4    |0      |0    |[4.0,0.0]   |[3.0,0.0]  |[74124.0,0.0]|[1.0,0.0]  |0.0       |
        // |506   |10   |27     |0    |[10.0,27.0] |[9.0,26.0] |[74124.0,0.0]|[1.0,0.0]  |0.0       |
        // +------+-----+-------+-----+------------+-----------+-------------+-----------+----------+
        predictionDF.show(10, truncate = false)
        
        /*
         * 3 按照用户 id 分区，统计每个用户购物男性或女性商品的个数及占比
         */
        val genderDF = predictionDF.groupBy($"userId").agg(
            // 某用户购买商品的总数
            count($"userId").as("total"),
            // 统计购买男性商品的个数
            sum(when($"prediction".equalTo(0), 1).otherwise(0)).as("maleTotal"),
            // 统计购买女性商品的个数
            sum(when($"prediction".equalTo(1), 1).otherwise(0)).as("femaleTotal")
        )
        
        // +------+-----+---------+-----------+
        // |userId|total|maleTotal|femaleTotal|
        // +------+-----+---------+-----------+
        // |530   |135  |97       |38         |
        // |857   |126  |93       |33         |
        // |506   |153  |120      |33         |
        // +------+-----+---------+-----------+
        // genderDF.show(3, truncate = false)
        
        /*
         * 4 计算标签，分别计算男性和女性商品的占比，当占比 >= 0.6 时确定购物性别
         */
        val rulesMap: Map[String, String] = TagTools.convertMap(tagDF)
        val rulesMapBroad = spark.sparkContext.broadcast(rulesMap)
        
        val gender_tag_udf = udf((total: Long, maleCount: Long, femaleCount: Long) => {
            val maleRate: Double = maleCount.toDouble / total
            val femaleRate: Double = femaleCount.toDouble / total
            
            // 这里是按照经验进行划分
            if (maleRate >= 0.6) {
                rulesMapBroad.value("0")
            } else if (femaleRate >= 0.6) {
                rulesMapBroad.value("1")
            } else {
                rulesMapBroad.value("-1")
            }
        })
        
        val modelDF = genderDF.select($"userId", gender_tag_udf($"total", $"maleTotal", $"femaleTotal").as("usg"))
        modelDF.show(100, truncate = false)
        
        modelDF
    }
    
    /**
      * 针对数据集进行特征工程：特征提取、特征转换及特征选择
      *
      * @param dataframe 数据集
      *
      * @return 数据集，包含特征列features: Vector类型和标签列label
      */
    def featuresTransform(dataframe: DataFrame): DataFrame = {
        // a. 特征向量化
        val assembler: VectorAssembler = new VectorAssembler()
            .setInputCols(Array("color", "product"))
            .setOutputCol("raw_features")
        val df1: DataFrame = assembler.transform(dataframe)
        
        // b. 类别特征进行索引
        val vectorIndexer: VectorIndexerModel = new VectorIndexer()
            .setInputCol("raw_features")
            .setOutputCol("features")
            .setMaxCategories(30)
            .fit(df1)
        val df2: DataFrame = vectorIndexer.transform(df1)
        
        // c. 返回特征数据
        df2
    }
    
    /**
      * 使用决策树分类算法训练模型，返回DecisionTreeClassificationModel模型
      *
      * @return
      */
    def trainModel(dataframe: DataFrame): DecisionTreeClassificationModel = {
        // a. 数据划分为训练数据集和测试数据集
        val Array(trainingDF, testingDF) = dataframe.randomSplit(Array(0.8,
            0.2), seed = 123L)
        
        // b. 构建决策树分类器
        val dtc: DecisionTreeClassifier = new DecisionTreeClassifier()
            .setFeaturesCol("features")
            .setLabelCol("label")
            .setPredictionCol("prediction")
            .setMaxDepth(5) // 树的深度
            .setMaxBins(32) // 树的叶子数目
            .setImpurity("gini") // 基尼系数
        
        // c. 训练模型
        logWarning("正在训练模型...................................")
        val dtcModel: DecisionTreeClassificationModel = dtc.fit(trainingDF)
        
        // d. 模型评估
        val predictionDF: DataFrame = dtcModel.transform(testingDF)
        println(s"accuracy = ${modelEvaluate(predictionDF, "accuracy")}")
        
        // e. 返回模型
        dtcModel
    }
    
    /**
      * 模型评估，返回计算分类指标值
      *
      * @param dataframe  预测结果的数据集
      * @param metricName 分类评估指标名称，支持：f1、weightedPrecision、weightedRecall、accuracy
      *                   准确率：针对全局数据预测结果的评估
      *                   精确率和召回率：???
      *
      * @return
      */
    def modelEvaluate(dataframe: DataFrame, metricName: String): Double = {
        // a. 构建多分类分类器
        val evaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("label")
            .setPredictionCol("prediction")
            // 指标名称，
            .setMetricName(metricName)
        
        // b. 计算评估指标
        val metric: Double = evaluator.evaluate(dataframe)
        
        // c. 返回指标
        metric
    }
    
    /**
      * 使用决策树分类算法训练模型，返回PipelineModel模型
      *
      * @return
      */
    def trainPipelineModel(dataframe: DataFrame): PipelineModel = {
        // 数据划分为训练数据集和测试数据集
        val Array(trainingDF, testingDF) = dataframe.randomSplit(Array(0.8, 0.2), seed = 123)
        
        // a. 特征向量化
        val assembler: VectorAssembler = new VectorAssembler()
            .setInputCols(Array("color", "product"))
            .setOutputCol("raw_features")
        
        // b. 类别特征进行索引
        val indexer: VectorIndexer = new VectorIndexer()
            .setInputCol("raw_features")
            .setOutputCol("features")
            .setMaxCategories(30)
        // .fit(dataframe)
        
        // c. 构建决策树分类器
        val dtc: DecisionTreeClassifier = new DecisionTreeClassifier()
            .setFeaturesCol("features")
            .setLabelCol("label")
            .setPredictionCol("prediction")
            // 树的深度
            .setMaxDepth(5)
            // 树的叶子数目
            .setMaxBins(32)
            // 基尼系数
            .setImpurity("gini")
        
        // d. 构建Pipeline管道流实例对象
        val pipeline: Pipeline = new Pipeline().setStages(
            Array(assembler, indexer, dtc)
        )
        
        // e. 训练模型
        val pipelineModel: PipelineModel = pipeline.fit(trainingDF)
        
        // f. 模型评估
        val predictionDF: DataFrame = pipelineModel.transform(testingDF)
        predictionDF.show(100, truncate = false)
        println(s"accuracy = ${modelEvaluate(predictionDF, "accuracy")}")
        
        // 返回模型
        pipelineModel
    }
}

object UsgModelMLPipeline {
    def main(args: Array[String]): Unit = {
        val model = new UsgModelMLPipeline()
        model.executeModel(378)
    }
}

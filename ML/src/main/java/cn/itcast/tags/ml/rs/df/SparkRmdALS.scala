package cn.itcast.tags.ml.rs.df

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
 * 使用 dataframe api 实现 ALS 算法库的使用，训练电影推荐模型
 */
object SparkRmdALS {
    
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
            .master("local[*]")
            .appName(this.getClass.getSimpleName.stripPrefix("$"))
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        
        import spark.implicits._
        
        // 自定义 schema 信息
        val mlSchema: StructType = StructType(
            Array(
                StructField("userId", IntegerType, nullable = true),
                StructField("movieId", IntegerType, nullable = true),
                StructField("rating", DoubleType, nullable = true),
                StructField("timestamp", LongType, nullable = true),
            )
        )
        
        // 读取电影评分数据，数据格式为 TSV
        val rawRatingDF = spark.read.format("csv")
            .option("sep", "\t")
            .schema(mlSchema)
            .csv("datas/als/ml-100k/u.data")
        
        // +------+-------+------+---------+
        // |userId|movieId|rating|timestamp|
        // +------+-------+------+---------+
        // |   196|    242|   3.0|881250949|
        // |   186|    302|   3.0|891717742|
        // |    22|    377|   1.0|878887116|
        // +------+-------+------+---------+
        // rawRatingDF.show(3)
        
        // Build the recommendation model using ALS on the training data
        val als = new ALS()
            // 迭代次数
            .setMaxIter(10)
            // 超参数
            .setRank(10)
            // 设置显示评分还是隐式评分
            .setImplicitPrefs(false)
            .setNumBlocks(4)
            .setNumUserBlocks(4)
            .setRegParam(0.01)
            .setUserCol("userId")
            .setItemCol("movieId")
            .setRatingCol("rating")
        
        val alsModel = als.fit(rawRatingDF)
        
        // 用户特征因子矩阵
        val userFactors: DataFrame = alsModel.userFactors
        userFactors.show(5, truncate = false)
        // 产品特征因子矩阵
        val itemFactors: DataFrame = alsModel.itemFactors
        itemFactors.show(5, truncate = false)
        
        /*
         * 模型评估
         */
        val predictions = alsModel.transform(rawRatingDF)
        val evaluator = new RegressionEvaluator()
            .setMetricName("rmse")
            .setLabelCol("rating")
            .setPredictionCol("prediction")
        
        // rmse: 0.6954456035703098
        val rmse = evaluator.evaluate(predictions)
        println("rmse: " + rmse)
        
        /*
         * 预测用户评分
         */
        
        // +------+-------+------+---------+----------+
        // |userId|movieId|rating|timestamp|prediction|
        // +------+-------+------+---------+----------+
        // |13    |12     |5.0   |881515011|4.8303823 |
        // |14    |12     |5.0   |890881216|4.476074  |
        // |18    |12     |5.0   |880129991|4.18575   |
        // |73    |12     |5.0   |888624976|4.5398936 |
        // |109   |12     |4.0   |880577542|4.2309237 |
        // +------+-------+------+---------+----------+
        predictions.show(50, truncate = false)
        
        /*
         * 为用户推荐物品
         */
        val userRecs = alsModel.recommendForAllUsers(3)
        // +------+-------------------------------------------------------+
        // |userId|recommendations                                        |
        // +------+-------------------------------------------------------+
        // |12    |[[1449, 5.6775107], [899, 5.5802126], [1266, 5.574067]]|
        // |13    |[[1425, 8.357606], [593, 6.582088], [982, 6.3163147]]  |
        // |14    |[[1169, 7.3257074], [57, 6.651092], [1056, 6.6381392]] |
        // |18    |[[1449, 5.4029055], [1426, 5.312996], [1558, 5.176868]]|
        // |25    |[[1449, 5.996713], [1005, 5.8638554], [394, 5.6955614]]|
        // +------+-------------------------------------------------------+
        userRecs.show(5, truncate = false)
        
        /*
         * 为电影推荐用户
         */
        val movieRecs = alsModel.recommendForAllItems(3)
        // +-------+-----------------------------------------------------+
        // |movieId|recommendations                                      |
        // +-------+-----------------------------------------------------+
        // |12     |[[143, 6.228756], [448, 6.1793327], [772, 6.1565456]]|
        // |13     |[[55, 6.9716673], [341, 6.5565877], [842, 5.9593225]]|
        // |14     |[[341, 7.3890905], [34, 6.1045384], [46, 6.0315447]] |
        // |18     |[[726, 8.0656595], [820, 7.217061], [762, 7.0414906]]|
        // |25     |[[688, 6.183156], [341, 5.8060045], [462, 5.220563]] |
        // +-------+-----------------------------------------------------+
        movieRecs.show(5, truncate = false)
        
        spark.stop()
    }
    
}

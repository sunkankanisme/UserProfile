package cn.itcast.tags.ml.regression

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
 * 使用线性回归算法对波士顿房价数据集构建模型
 */
object LrBostonRegression {

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
            .master("local[*]")
            .appName(this.getClass.getSimpleName.stripPrefix("$"))
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()

        import spark.implicits._

        /*
         * 加载数据集
         */
        val bostonPriceDF = spark.read.textFile("datas/housing/housing.data")
            .filter(line => null != line && line.trim.split("\\s+").length == 14)

        /*
         * 获取特征和标签
         */
        val bostonDF: DataFrame = bostonPriceDF.mapPartitions(iter => {
            iter.map(line => {
                val strings = line.trim.split("\\s+")
                // 获取标签 label
                val label: Double = strings(strings.length - 1).toDouble
                // 获取特征 features
                val features: Vector = Vectors.dense(strings.dropRight(1).map(_.toDouble))

                (features, label)
            })
        }).toDF("features", "label")

        bostonDF.printSchema()
        bostonDF.show(10, truncate = false)

        /*
         * 特征数据的转换处理
         * 注：此处直接使用算法中的参数对数据进行标准化即可
         */


        /*
         * 划分数据集为训练集和测试集
         */
        val Array(trainingDF, testingDF) = bostonDF.randomSplit(Array(0.8, 0.2))
        trainingDF.cache()


        /*
         * 创建线性回归算法实例对象，应用数据训练模型
         */
        val lr = new LinearRegression()
            .setFeaturesCol("features")
            .setLabelCol("label")
            .setStandardization(true)
            // 超参数设置，最大迭代次数
            .setMaxIter(50)
            // 设置算法底层的求解方式，最小二乘法（normal）或拟牛顿法（l-bfgs），填入 auto 自动选择
            .setSolver("auto")
            // 针对最小二乘法的参数
            .setRegParam(1)
            .setElasticNetParam(0.4)

        val regressionModel = lr.fit(trainingDF)

        /*
         * 模型的评估
         */
        println("斜率: " + regressionModel.coefficients)
        println("截距: " + regressionModel.intercept)

        val summary = regressionModel.summary
        // 均方根误差，越小越好
        println("RMSE: " + summary.rootMeanSquaredError)
        println("r2: " + summary.r2)

        // 使用测试集进行预测
        // +---------------------------------------------------------------------------+-----+------------------+
        // |features                                                                   |label|prediction        |
        // +---------------------------------------------------------------------------+-----+------------------+
        // |[0.00906,90.0,2.97,0.0,0.4,7.088,20.8,7.3073,1.0,285.0,15.3,394.72,7.85]   |32.2 |32.400132096579895|
        // |[0.01301,35.0,1.52,0.0,0.442,7.241,49.3,7.0379,1.0,284.0,15.5,394.74,5.49] |32.7 |30.234022947973052|
        // |[0.01439,60.0,2.93,0.0,0.401,6.604,18.8,6.2196,1.0,265.0,15.6,376.7,4.38]  |29.1 |32.342710749468814|
        // |[0.01709,90.0,2.02,0.0,0.41,6.728,36.1,12.1265,5.0,187.0,17.0,384.46,4.5]  |30.1 |24.513883413538906|
        // |[0.01951,17.5,1.38,0.0,0.4161,7.104,59.5,9.2229,3.0,216.0,18.6,393.24,8.05]|33.0 |22.30397296901143 |
        // |[0.02009,95.0,2.68,0.0,0.4161,8.034,31.9,5.118,4.0,224.0,14.7,390.55,2.88] |50.0 |43.87941502185342 |
        // |[0.02187,60.0,2.93,0.0,0.401,6.8,9.9,6.2196,1.0,265.0,15.6,393.37,5.03]    |31.1 |33.00409270382178 |
        // |[0.02543,55.0,3.78,0.0,0.484,6.696,56.4,5.7321,5.0,370.0,17.6,396.9,7.18]  |23.9 |27.70701739270354 |
        // |[0.02899,40.0,1.25,0.0,0.429,6.939,34.5,8.7921,1.0,335.0,19.7,389.85,5.89] |26.6 |21.5230146222892  |
        // |[0.03049,55.0,3.78,0.0,0.484,6.874,28.1,6.4654,5.0,370.0,17.6,387.97,4.61] |31.2 |28.682833381033397|
        // +---------------------------------------------------------------------------+-----+------------------+
        regressionModel.transform(testingDF).show(10, truncate = false)

        spark.stop()
    }

}

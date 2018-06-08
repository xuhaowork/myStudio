package com.self.core.featurePretreatment

import com.self.core.baseApp.myAPP
import com.self.core.featurePretreatment.models.{OnlyOneDFOutput, Pretreater}
import org.apache.spark.sql.DataFrame


/**
  * editor: xuhao
  * date: 2018-06-08 08:30:00
  */
object FeaturePretreatment extends myAPP {
  override def run(): Unit = {
    /** 一些基本的参数设定和平台的变量 */
    //    val jsonParam = "<#jsonparam#>"
    //    val gson = new Gson()
    //    val p: java.util.Map[String, String] =
    //      gson.fromJson(jsonParam, classOf[java.util.Map[String, String]])
    //    val z1 = z
    //    val rddTableName = "<#rddtablename#>"
    //    val tableName = p.get("inputTableName").trim
    //    val rawDataFrame = z1.rdd(tableName).asInstanceOf[DataFrame]


    val rawDataFrame = sqlc.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (0, "I wish Java could use case classes"),
      (1, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    val inputCol = "sentence"
    val outputCol = "sentenceOut"
    val numFeatures = 10000

    /** 测试一下 */
    class Pretreatment(override val data: DataFrame) extends Pretreater[OnlyOneDFOutput](data) {
      override protected def paramsValid(): Boolean = true

      /**
        * 执行函数接口
        *
        * @return 输出一个PretreatmentOutput的实现子类型
        */
      override protected def runAlgorithm(): OnlyOneDFOutput = {
        import org.apache.spark.ml.feature.Tokenizer

        /**
          * 约定
          * ----
          * numFeatures hash后的特征数
          */

        val inputCol = this.getParams[String]("inputCol", "输入列名")
        val outputCol = this.getParams("outputCol", "输出列名")
        val numFeatures = this.getParams[Int]("numFeatures", "hash后的特征数")

        val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
        val wordsData = tokenizer.transform(this.data)
        new OnlyOneDFOutput(wordsData)
      }
    }


    new Pretreatment(rawDataFrame)
      .setParams(inputCol, "inputCol").setParams(outputCol, "outputCol").setParams(1, "numFeatures").run().outputData.show()






  }

}

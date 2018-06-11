package com.self.core.featurePretreatment

import com.self.core.baseApp.myAPP
import com.self.core.featurePretreatment.models.{CountWordVector, CountWordVectorParamsName, TokenizerByRegex, TokenizerByRegexParamsName}
import org.apache.spark.sql.DataFrame


/**
  * editor: xuhao
  * date: 2018-06-08 08:30:00
  */
object FeaturePretreatment extends myAPP {

  object data1 {
    val data: DataFrame = {
      sqlc.createDataFrame(Seq(
        (0, "Hi I heard about Spark"),
        (0, "I wish Java could use case classes, which i like mostly"),
        (1, "Does Logistic regression models has a implicit params, Halt?")
      )).toDF("label", "sentence")
    }

    val inputCol = "sentence"
    val outputCol = "sentenceOut"
  }

  object data2 {
    val data: DataFrame = {
      sqlc.createDataFrame(Seq(
        (0, "Hello，小李子"),
        (0, "今天吃了吗？"),
        (1, "谁今天要请我吃饭？")
      )).toDF("label", "sentence")
    }

    val inputCol = "sentence"
    val outputCol = "sentenceOut"
  }


  object data3 {
    val data: DataFrame = sqlc.createDataFrame(Seq(
      (0, Array("a", "b", "c", "d")),
      (1, Array("a", "b", "b", "c", "a"))
    )).toDF("id", "letters")

    val data2: DataFrame = sqlc.createDataFrame(Seq(
      (0, Array("a", "x", "f", "good")),
      (1, Array("a", "f", "b", "2", "a"))
    )).toDF("index", "words")

    val inputCol = "sentence"
    val outputCol = "sentenceOut"
  }

  /** 1.正则表达式分词 */
  def test1() = {
    val testData = data1
    val rawDataFrame = testData.data

    val inputCol = testData.inputCol
    val outputCol = testData.outputCol
    val gaps = false
    val minTokenLength = 0
    val pattern = """[Ii].*[Hh]"""

    val newDataFrame = new TokenizerByRegex(rawDataFrame)
      .setParams(TokenizerByRegexParamsName.inputCol, inputCol)
      .setParams(TokenizerByRegexParamsName.outputCol, outputCol)
      .setParams(TokenizerByRegexParamsName.gaps, gaps)
      .setParams(TokenizerByRegexParamsName.minTokenLength, minTokenLength)
      .setParams(TokenizerByRegexParamsName.toLowerCase, false)
      .setParams(TokenizerByRegexParamsName.pattern, pattern)
      .run()
      .outputData

    println(newDataFrame.schema)
    newDataFrame.show()
  }


  /** 2.词频统计 */
  def test2() = {
    data3.data.show()

    val mediumCols = "ff" + "_"

    // 先全写上，后面用到啥写啥就可以运行了
    val newDataFrame = new CountWordVector(data3.data)
      .setParams(CountWordVectorParamsName.inputCol, mediumCols)
      .setParams(CountWordVectorParamsName.outputCol, "")
      .setParams(CountWordVectorParamsName.trainData, data3.data2)
      .setParams(CountWordVectorParamsName.trainInputCol, "words")
      .setParams(CountWordVectorParamsName.trainOutputCol, mediumCols)
      .setParams(CountWordVectorParamsName.loadModel, true)
      .setParams(CountWordVectorParamsName.saveModel, true)
      .setParams(CountWordVectorParamsName.vocabSize, 1 << 18)
      .setParams(CountWordVectorParamsName.minTf, 1.0)
      .setParams(CountWordVectorParamsName.minDf, 1.0)
      .setParams(CountWordVectorParamsName.savePath, "/data/wordCount/...")
      .run()
      .outputData

    newDataFrame.show()
  }


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


//    test1()

//    test2()




  }

}

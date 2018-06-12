package com.self.core.featurePretreatment

import com.self.core.baseApp.myAPP
import com.self.core.featurePretreatment.models._
import org.apache.spark.ml.feature.Tokenizer
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


    val data1 = {
      val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
      val wordsData = tokenizer.transform(data)
      wordsData // label, words -- Seq[String]
    }
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

  object data4 {
    val data: DataFrame = sqlc.createDataFrame(Seq(
      (0, Seq("I", "saw", "the", "red", "baloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id", "raw")

  }

  object data5 {
    val wordDataFrame: DataFrame = sqlc.createDataFrame(Seq(
      (0, Array("Hi", "I", "heard", "about", "Spark")),
      (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
      (2, Array("Logistic", "regression", "models", "are", "neat"))
    )).toDF("label", "words")

    val inputCol = "words"
  }


  /** 一、属性类型特征提取 */
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


  /** 3.hash词频统计 */
  def test3() = {

    import com.self.core.featurePretreatment.models.{HashTF, HashTFParamsName}

    val sentenceData = data1.data1
    val numFeature = 10000 // 在由string转过来的时候需要判断一下是否越界
    val inputCol = "words"
    val outputCol = "wordsOutput"


    val newDataFrame = new HashTF(sentenceData).setParams(HashTFParamsName.numFeatures, numFeature)
      .setParams(HashTFParamsName.inputCol, inputCol)
      .setParams(HashTFParamsName.outputCol, outputCol)
      .run()

    newDataFrame.outputData.show()

  }


  /** 4.Word2Vector */
  def test4() = {
    val rawDataFrame = data1.data1
    rawDataFrame.show()

    import com.self.core.featurePretreatment.models.{WordToVector, WordToVectorParamsName}

    val newDataFrame = new WordToVector(rawDataFrame)
      .setParams(WordToVectorParamsName.inputCol, "words")
      .setParams(WordToVectorParamsName.outputCol, "wordsOutput")
      .setParams(WordToVectorParamsName.windowSize, 5)
      .setParams(WordToVectorParamsName.vocabSize, 1000)
      .setParams(WordToVectorParamsName.stepSize, 0.1)
      //      .setParams(WordToVectorParamsName.trainData, rawDataFrame)
      //      .setParams(WordToVectorParamsName.trainInputCol, "") // 在内部中会验证是否在trainData中
      //      .setParams(WordToVectorParamsName.trainOutputCol, "") // 没有就不要写，否则训练找不到
      .setParams(WordToVectorParamsName.minCount, 1)
      .setParams(WordToVectorParamsName.numPartitions, 1)
      .setParams(WordToVectorParamsName.numIterations, 10)
      .setParams(WordToVectorParamsName.loadModel, false)
      .setParams(WordToVectorParamsName.loadPath, "")
      .setParams(WordToVectorParamsName.saveModel, false)
      .setParams(WordToVectorParamsName.savePath, "")
      .run()
      .outputData

    newDataFrame.show()

    println(newDataFrame.select("wordsOutput").collect().head.getAs[org.apache.spark.mllib.linalg.Vector](0).size)

  }


  /** 5. */
  def test5() = {
    import com.self.core.featurePretreatment.models.{StopWordsRmv, StopWordsRemoverParamsName}
    import com.self.core.featurePretreatment.models.StopWordsUtils

    val stopWordsFormat = "English" // "chinese", "byHand", "byFile"

    val stopWords = stopWordsFormat match {
      case "English" => StopWordsUtils.English
      case "Chinese" => StopWordsUtils.chinese
      case "byHand" => Array("", "")
      case "byFile" =>
        val path = "/data/dfe"
        val separator = ","
        sc.textFile(path).collect().flatMap(_.split(separator).map(_.trim))
    }

    val rawDataFrame = data4.data
    val newDataFrame = new StopWordsRmv(rawDataFrame)
      .setParams(StopWordsRemoverParamsName.inputCol, "raw")
      .setParams(StopWordsRemoverParamsName.outputCol, "rawOutput")
      .setParams(StopWordsRemoverParamsName.caseSensitive, false)
      .setParams(StopWordsRemoverParamsName.stopWords, stopWords)
      .run().outputData
    newDataFrame.show()
    println(StopWordsUtils.English.length)


  }

  def test6() = {
    /** n-gram */
    import com.self.core.featurePretreatment.models.{NGramMD, NGramParamsName}

    val rawDataFrame = data5.wordDataFrame
    val inputCol = data5.inputCol

    val newDataFrame = new NGramMD(rawDataFrame)
      .setParams(NGramParamsName.inputCol, inputCol)
      .setParams(NGramParamsName.outputCol, "wordsOutput")
      .setParams(NGramParamsName.n, 3)
      .run()
      .outputData
    newDataFrame.show()

  }



  /** 二、数值类型特征提取 */
  def test7() = {




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

    //    test3()

    //    test4()

    //    test5()

    //    test6()


    println(sc.getConf.getOption("spark.akka.frameSize").isDefined)

    println(true ^ false)
    println(false ^ true)
    println(true ^ true)
    println(false ^ false)


  }

}

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

  object data6 {
    val data: DataFrame = sqlc.createDataFrame(
      Array((0, 18.0), (1, 19.0), (2, 8.0), (3, 5.0), (4, 2.2))
    ).toDF("id", "hour")
  }

  object data7 {
    val data: DataFrame = {
      import com.self.core.featurePretreatment.models.{Discretizer, DiscretizerParams}
      val rawDataFrame = data6.data

      new Discretizer(rawDataFrame)
        .setParams(DiscretizerParams.inputCol, "hour")
        .setParams(DiscretizerParams.outputCol, "outputCol")
        .setParams(DiscretizerParams.discretizeFormat, "byWidth")
        .setParams(DiscretizerParams.phase, 0.0)
        .setParams(DiscretizerParams.width, 3.0)
        .run()
        .data
    }
  }


  object data8 {

    import org.apache.spark.mllib.linalg.Vectors

    val data: DataFrame = {
      sqlc.createDataFrame(
        Array(
          Vectors.dense(2.0, 1.0),
          Vectors.dense(0.0, 3.0),
          Vectors.dense(0.0, -3.0),
          Vectors.dense(2.0, 2.0)).map(Tuple1.apply)
      ).toDF("features")
    }

  }

  object data9 {

    import org.apache.spark.mllib.linalg.Vectors

    val data: DataFrame = sqlc.createDataFrame(
      Array(
        Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
        Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
        Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
      ).map(Tuple1.apply)
    ).toDF("pcaFeature")

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
      .data

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
      .data

    newDataFrame.show()
  }


  /** 3.hash词频统计 */
  def test3() = {

    import com.self.core.featurePretreatment.models.{HashTF, HashTFParamsName}

    val sentenceData = data1.data1
    val numFeature = 10000 // 在由string转过来的时候需要判断一下是否越界
    val inputCol = "words"
    val outputCol = "wordsOutput"


    val newDataFrame = new HashTF(sentenceData)
      .setParams(HashTFParamsName.numFeatures, numFeature)
      .setParams(HashTFParamsName.inputCol, inputCol)
      .setParams(HashTFParamsName.outputCol, outputCol)
      .run()

    newDataFrame.data.show()
    newDataFrame.data
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
      .data

    newDataFrame.show()

    println(newDataFrame.select("wordsOutput").collect().head.getAs[org.apache.spark.mllib.linalg.Vector](0).size)

  }


  /** 5. */
  def test5() = {
    import com.self.core.featurePretreatment.models.{StopWordsRemoverParamsName, StopWordsRmv, StopWordsUtils}

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
      .run().data
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
      .data
    newDataFrame.show()

  }


  /** 二、数值类型特征提取 */
  def test7() = {
    import com.self.core.featurePretreatment.models.{Discretizer, DiscretizerParams}
    val rawDataFrame = data6.data

    val byWidthDF = new Discretizer(rawDataFrame)
      .setParams(DiscretizerParams.inputCol, "hour")
      .setParams(DiscretizerParams.outputCol, "outputCol")
      .setParams(DiscretizerParams.discretizeFormat, "byWidth")
      .setParams(DiscretizerParams.phase, 0.0)
      .setParams(DiscretizerParams.width, 4.0)
      .run()
      .data

    byWidthDF.show()

    val byDepthDF = new Discretizer(rawDataFrame)
      .setParams(DiscretizerParams.inputCol, "hour")
      .setParams(DiscretizerParams.outputCol, "outputCol")
      .setParams(DiscretizerParams.discretizeFormat, "byDepth")
      .setParams(DiscretizerParams.depth, 2.0) // @todo 必须是Double，否则java.lang.Integer cannot be cast to java.lang.Double
      .run()
      .data

    byDepthDF.show()

    val byDepthDF2 = new Discretizer(rawDataFrame)
      .setParams(DiscretizerParams.inputCol, "hour")
      .setParams(DiscretizerParams.outputCol, "outputCol")
      .setParams(DiscretizerParams.discretizeFormat, "byDepth")
      .setParams(DiscretizerParams.boxesNum, 4.0) // @todo 必须是Double，否则java.lang.Integer cannot be cast to java.lang.Double
      .run()
      .data

    byDepthDF2.show()

    val byDepthDF3 = new Discretizer(rawDataFrame)
      .setParams(DiscretizerParams.inputCol, "hour")
      .setParams(DiscretizerParams.outputCol, "outputCol")
      .setParams(DiscretizerParams.discretizeFormat, "selfDefined")
      .setParams(DiscretizerParams.buckets, Array(3.0, 9.0, 5.0))
      .setParams(DiscretizerParams.bucketsAddInfinity, true)
      .run()
      .data

    byDepthDF3.show()

  }


  def test8() = {
    val rawDataFrame = data7.data
    rawDataFrame.show()


    import com.self.core.featurePretreatment.models.{OneHotCoder, OneHotCoderParams}

    val newDataFrame = new OneHotCoder(rawDataFrame)
      .setParams(OneHotCoderParams.inputCol, "outputCol")
      .setParams(OneHotCoderParams.outputCol, "oneHot")
      .setParams(OneHotCoderParams.dropLast, true)
      .run()
      .data

    newDataFrame.show()

    newDataFrame.select("oneHot").rdd
      .map(_.get(0).asInstanceOf[org.apache.spark.mllib.linalg.Vector])
      .collect()
      .foreach(println)

  }


  def test9() = {
    import com.self.core.featurePretreatment.models.{IDFTransformer, IDFTransformerParams}
    val rawDataFrame = test3()

    val inputCol = "wordsOutput"

    rawDataFrame.select(inputCol).rdd
      .map(_.get(0).asInstanceOf[org.apache.spark.mllib.linalg.Vector])
      .collect()
      .foreach(println)

    val outputCol = "idfCol"
    val newDataFrame = new IDFTransformer(rawDataFrame)
      .setParams(IDFTransformerParams.inputCol, inputCol)
      .setParams(IDFTransformerParams.outputCol, outputCol)
      .setParams(IDFTransformerParams.loadModel, false)
      .setParams(IDFTransformerParams.minDocFreq, 1) // @todo: 必须为Int
      .setParams(IDFTransformerParams.saveModel, false)
      .run()
      .data

    newDataFrame.select(outputCol).rdd
      .map(_.get(0).asInstanceOf[org.apache.spark.mllib.linalg.Vector])
      .collect()
      .foreach(println)
    newDataFrame
  }

  def test10() = {
    import com.self.core.featurePretreatment.models.{VectorIndexerParams, VectorIndexerTransformer}

    val rawDataFrame = data8.data

    rawDataFrame.show()

    val newDataFrame = new VectorIndexerTransformer(rawDataFrame)
      .setParams(VectorIndexerParams.inputCol, "features")
      .setParams(VectorIndexerParams.outputCol, "outPut")
      .setParams(VectorIndexerParams.loadModel, false)
      .setParams(VectorIndexerParams.maxCategories, 2) // @todo
      .setParams(VectorIndexerParams.loadModel, false)
      .run()
      .data

    newDataFrame.show()

  }


  def test11() = {
    import com.self.core.featurePretreatment.models.{PCAParams, PCATransformer}
    val rawDataFrame = data9.data
    val inputCol = "pcaFeature"
    rawDataFrame.show()
    val newDataFrame = new PCATransformer(rawDataFrame)
      .setParams(PCAParams.inputCol, inputCol)
      .setParams(PCAParams.outputCol, "output")
      .setParams(PCAParams.loadModel, false)
      .setParams(PCAParams.saveModel, false)
      .setParams(PCAParams.p, 2) // 需要小于向量长度
      .run()
      .data
    newDataFrame.show()
  }

  def test12() = {
    import com.self.core.featurePretreatment.models.{PlynExpansionParams, PlynExpansionTransformer}
    val rawDataFrame = data9.data
    val inputCol = "pcaFeature"
    rawDataFrame.show()

    val newDataFrame = new PlynExpansionTransformer(rawDataFrame)
      .setParams(PlynExpansionParams.inputCol, inputCol)
      .setParams(PlynExpansionParams.outputCol, "output")
      .setParams(PlynExpansionParams.degree, 3)
      .run()
      .data
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

    //    test3()

    //    test4()

    //    test5()

    //    test6()

    //    test7()

    //    test8()

    //    test9()

    //    test10()

    //    test11()

    test12()


    //    println(sc.getConf.getOption("spark.akka.frameSize").isDefined)
    //
    //    println(true ^ false)
    //    println(false ^ true)
    //    println(true ^ true)
    //    println(false ^ false)


  }

}

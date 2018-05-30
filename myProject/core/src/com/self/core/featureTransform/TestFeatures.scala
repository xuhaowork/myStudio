package com.self.core.featureTransform

import breeze.linalg.{DenseVector => BDV}
import com.self.core.baseApp.myAPP

object TestFeatures extends myAPP {
  def feature11(): Unit = {
    /** 2-3 tf-idf转换 */
    import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

    val sentenceData = sqlc.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (0, "I wish Java could use case classes"),
      (1, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)
    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(10000) // 取模值
    val featurizedData = hashingTF.transform(wordsData)
    // CountVectorizer也可获取词频向量

    featurizedData.show()

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features").setMinDocFreq(1) // 最低统计词频
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("features", "label").take(3).foreach(println)
  }

  def feature12(): Unit = {
    /** 2-1  Word2Vec */
    import org.apache.spark.ml.feature.Word2Vec

    // Input data: Each row is a bag of words from a sentence or document.
    val documentDF = sqlc.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(documentDF)
    val result = model.transform(documentDF)
    result.select("result").take(3).foreach(println)
  }

  def feature13() = {
    /** 1-1 正则分词器 */
    import org.apache.spark.ml.feature.RegexTokenizer

    val sentenceDataFrame = sqlc.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (1, "I wish Java could use case classes"),
      (2, "Logistic,regression,models,are,neat")
    )).toDF("label", "sentence")

    //    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("words").setToLowercase(true).setGaps(true).setMinTokenLength(2)
      .setPattern("\\W") // alternatively .setPattern("\\w+").setGaps(false)

    //    val tokenized = tokenizer.transform(sentenceDataFrame)
    //    tokenized.select("words", "label").take(3).foreach(println)
    val regexTokenized = regexTokenizer.transform(sentenceDataFrame)
    println(regexTokenized.schema.map(_.dataType).mkString(","))
    //    regexTokenized.select("words", "label").take(3).foreach(println)
  }


  def feature14() = {
    /** 2-2 向量计数器 */
    import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}

    val df = sqlc.createDataFrame(Seq(
      (0, Array("a", "b", "c")),
      (1, Array("a", "b", "b", "c", "a"))
    )).toDF("id", "words")

    // fit a CountVectorizerModel from the corpus
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(3)
      .setMinDF(2)
      .setMinTF(1)
      .fit(df)

    cvModel.transform(df).select("features").show()
  }

  def feature15() = {
    import org.apache.spark.ml.feature.StopWordsRemover

    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered").setStopWords(Array("I", "Mary"))


    val dataSet = sqlc.createDataFrame(Seq(
      (0, Seq("I", "saw", "the", "red", "baloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id", "raw")

    remover.transform(dataSet).show()
  }

  override def run(): Unit = {
    /** 1.特征提取 */
    /** 1-1 tf-idf转换 */
    //    feature11() //
    /** 1-2  Word2Vec */
    //    feature12()

    /** 1-3 正则分词器 */
    //    feature13()

    /** 1-4 向量计数器 */
    //    feature14()

    /** 1-5 停用词移除 */
    feature15()


    //    import org.apache.spark.ml.feature.DCT
    //    import org.apache.spark.mllib.linalg.{Vectors, Vector}
    //
    //    val data = Seq(
    //      Vectors.dense(0.0, 1.0, -2.0, 3.0),
    //      Vectors.dense(-1.0, 2.0, 4.0, -7.0),
    //      Vectors.dense(14.0, -2.0, -5.0, 1.0))
    //
    //    val df = sqlc.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    //    println(df.schema)
    //
    //
    //
    //    val dct = new DCT()
    //      .setInputCol("features")
    //      .setOutputCol("featuresDCT")
    //      .setInverse(false)
    //
    //    val dctDf = dct.transform(df)
    //    dctDf.take(1).map(r => r.getAs[Vector](0)).foreach(println)
    //


    //    /** one-hot编码 */
    //    import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
    //
    //    val df2 = sqlc.createDataFrame(Seq(
    //      (0, "a"),
    //      (1, "b"),
    //      (2, "c"),
    //      (3, "a"),
    //      (4, "a"),
    //      (5, "c")
    //    )).toDF("id", "category")
    //
    //
    //    val df3 = sqlc.createDataFrame(Seq(
    //      (0, "a"),
    //      (1, "b"),
    //      (2, "c"),
    //      (3, "d"),
    //      (4, "a"),
    //      (5, "c")
    //    )).toDF("someId", "category")
    //
    //    val indexer = new StringIndexer()
    //      .setInputCol("category")
    //      .setOutputCol("categoryIndex")
    //      .fit(df2)
    //    val indexed = indexer.transform(df2)
    //    println(indexer.labels.mkString(","))
    //
    //    indexed.show()
    //    val encoder = new OneHotEncoder()
    //      .setInputCol("categoryIndex")
    //      .setOutputCol("categoryVec").setDropLast(false)
    //    val encoded = encoder.transform(indexed)
    //    encoded.select("id", "categoryVec").show()


    /** hashing-TF */


  }
}

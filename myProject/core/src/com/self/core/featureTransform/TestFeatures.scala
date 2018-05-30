package com.self.core.featureTransform

import breeze.linalg.{DenseVector => BDV}
import breeze.stats.distributions.Gaussian
import com.self.core.baseApp.myAPP
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, substring}
import org.apache.spark.sql.types.DoubleType

import scala.Serializable

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
      .setOutputCol("filtered")
      .setStopWords(Array("I", "Mary")) // 停用词


    val dataSet: DataFrame = sqlc.createDataFrame(Seq(
      (0, Seq("I", "saw", "the", "red", "baloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id", "raw")

    remover.transform(dataSet).show()
  }

  def feature16() = {
    /** n-gram */
    import org.apache.spark.ml.feature.NGram

    val wordDataFrame = sqlc.createDataFrame(Seq(
      (0, Array("Hi", "I", "heard", "about", "Spark")),
      (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
      (2, Array("Logistic", "regression", "models", "are", "neat"))
    )).toDF("label", "words")

    val ngram = new NGram().setInputCol("words").setOutputCol("ngrams").setN(3)
    val ngramDataFrame = ngram.transform(wordDataFrame)
    ngramDataFrame.take(3).map(_.getAs[Stream[String]]("ngrams").toList).foreach(println)
    println(ngramDataFrame.schema.map(_.dataType).mkString(","))
  }

  def feature17() = {
    /** 二值化 */
    import org.apache.spark.ml.feature.Binarizer

    val data = Array((0, "0.1"), (1, "0.8"), (2, null))
    val dataFrame = sqlc.createDataFrame(data).toDF("label", "feature").select(col("feature").cast(DoubleType))
    dataFrame.show()
    val binarizer: Binarizer = new Binarizer()
      .setInputCol("feature") // 此处必须是Double类型，还需要一加一个转换。null值是可以的
      .setOutputCol("binarized_feature")
      .setThreshold(0.5)

    val binarizedDataFrame = binarizer.transform(dataFrame)
    val binarizedFeatures = binarizedDataFrame.select("binarized_feature")
    binarizedFeatures.collect().foreach(println)
  }

  def feature18() = {
    import org.apache.spark.ml.feature.PCA
    import org.apache.spark.mllib.linalg.Vectors

    val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    )
    val df = sqlc.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(3)
      .fit(df)
    val pcaDF = pca.transform(df)
    val result = pcaDF.select("pcaFeatures")
    result.show()
  }

  def feature19() = {
    import org.apache.spark.ml.feature.PolynomialExpansion
    import org.apache.spark.mllib.linalg.Vectors

    val data = Array(
      Vectors.dense(-2.0, 1.0),
      Vectors.dense(0.0, 3.0),
      Vectors.dense(0.0, -3.0),
      Vectors.dense(1.0, 2.0)
    )
    val df = sqlc.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    val polynomialExpansion = new PolynomialExpansion()
      .setInputCol("features")
      .setOutputCol("polyFeatures")
      .setDegree(3)
    val polyDF = polynomialExpansion.transform(df)
    polyDF.select("polyFeatures").take(4).foreach(println)

  }

  def feature20() = {
    import org.apache.spark.ml.feature.DCT
    import org.apache.spark.mllib.linalg.Vectors

    val data = Seq(
      Vectors.dense(0.0, 1.0, -2.0),
      Vectors.dense(-1.0, 2.0, 4.0, -7.0),
      Vectors.dense(14.0))

    val df = sqlc.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val dct = new DCT()
      .setInputCol("features")
      .setOutputCol("featuresDCT")
      .setInverse(false)

    val dctDf = dct.transform(df)

    dctDf.select("featuresDCT").show(3)
  }

  def feature21() = {
    import org.apache.spark.ml.feature.StringIndexer

    val df1 = sqlc.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"))
    ).toDF("id", "category")

    val df2 = sqlc.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    ).toDF("id", "category")


    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")

    val indexed = indexer.fit(df1).transform(df2)
    indexed.show()
  }

  def feature22() = {
    import org.apache.spark.ml.feature.{IndexToString, StringIndexer}

    val df = sqlc.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "c")
    )).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
      .fit(df)
    val indexed = indexer.transform(df)

    val converter = new IndexToString()
      .setInputCol("categoryIndex")
      .setOutputCol("originalCategory")

    val converted = converter.transform(indexed)
    converted.select("id", "originalCategory").show()
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
//    feature15()

    /** 1-6 n-gram */
//    feature16()

    /** 1-7 二值化 */
//    feature17()

    /** 1-8 主成分 */
//    feature18()

    /** 1-9 多项式展开 */
//    feature19()

    /** 1-10 离散余弦变换 */
//    feature20()

    /** 1-11 String标签转为数值标签 */
//    feature21()

    /** 1-12 Double标签转为String标签 */
//    feature22()


val gaussian = new Gaussian(0.0, 1.0)

    println(gaussian.cdf(0.0))
    println(gaussian.pdf(0.0))

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
        val df2 = sqlc.createDataFrame(Seq(
          (0, "2018-05-28 00:00:00"),
          (1, "2018-05-28 00:00:00"),
          (2, "2018-05-28 00:00:00"),
          (3, "2018-05-28 00:00:00"),
          (4, "2018-05-28 00:00:00"),
          (5, "2018-05-28 00:00:00")
        )).toDF("id", "category")
    df2.withColumn("new", substring(col("category"), 9, 2))
    //
    //
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

//
//    class Item[-A](a: A){
//      def check(a: A) = true
//    }
//    def getVolvo(v: Item[Volvo]) = {
////      v.check(new VolvoWagon)
//      true
//    }
//    val car1: Item[Car] = new Item[Car](new Car)
//    val volvo1: Item[Volvo] = new Item(new Volvo)
//    getVolvo(car1)
//    getVolvo(volvo1)


//    class Item[-A](a: A){
//      def check(a: A) = true
//    }
//    val car1: Item[Car] = new Item(new Car)
//    val volvo1: Item[Volvo] = new Item(new Volvo)
//
//    def getVolvo(volvo: Item[Volvo]) = true
//    println(getVolvo(volvo1)) // 类型恰当
//    println(getVolvo(car1)) // compiled，需要Item[+A]中Car转为了父类型Volvo，基类Car向下兼容为了子类Volvo，发生了逆变



    //    /** 协变 */
//    def getCar(car: Item[Car]) = true
//    println(getCar(car1)) // 类型恰当
//    println(getCar(volvo1)) // compile， 需要Item[+A]中Volvo转为了父类型Car，子类Volvo向上兼容为基类Car，发生了协变
//


  }
}

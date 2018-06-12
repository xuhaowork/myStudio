package com.self.core.featureTransform


import com.self.core.baseApp.myAPP
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, substring}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField}

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
      .setOutputCol("categoryIndex").setHandleInvalid("skip")
      .fit(df)
    val indexed = indexer.transform(df)

    val converter = new IndexToString()
      .setInputCol("categoryIndex")
      .setOutputCol("originalCategory")

    val converted = converter.transform(indexed)
    converted.select("id", "originalCategory").show()
  }

  def feature23() = {
    import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}

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

    indexed.show()
    val encoder = new OneHotEncoder()
      .setInputCol("categoryIndex")
      .setOutputCol("categoryVec").setDropLast(false)
    val encoded = encoder.transform(indexed)
    encoded.select("id", "categoryVec").show()
  }

  def feature24() = {
    import org.apache.spark.ml.feature.VectorIndexer

    val data = sqlc.read.format("libsvm").load("F:/scala-spark/lib/spark-2.0.0-bin-hadoop2.6/spark-2.0.0-bin-hadoop2.6/data//mllib/sample_libsvm_data.txt")

    data.show()

    val indexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexed")
      .setMaxCategories(10)

    val indexerModel = indexer.fit(data)

    val categoricalFeatures: Set[Int] = indexerModel.categoryMaps.keys.toSet
    println(s"Chose ${categoricalFeatures.size} categorical features: " +
      categoricalFeatures.mkString(", "))

    // Create new column "indexed" with categorical values transformed to indices
    val indexedData = indexerModel.transform(data)
    indexedData.show()
  }


  def feature25() = {
    /** 正则化 */
    import org.apache.spark.ml.feature.Normalizer

    val data = Seq(
      Vectors.dense(0.0, 1.0, -2.0),
      Vectors.dense(-1.0, 2.0, 4.0, -7.0),
      Vectors.dense(14.0))

    val dataFrame = sqlc.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    dataFrame.show()

    // Normalize each Vector using $L^1$ norm.
    val normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures")
      .setP(2.0)

    val l1NormData = normalizer.transform(dataFrame)
    l1NormData.show()

    // Normalize each Vector using $L^\infty$ norm.
    val lInfNormData = normalizer.transform(dataFrame, normalizer.p -> Double.PositiveInfinity)
    lInfNormData.show()
  }

  def feature26() = {
    /** 标准化 */
    /** z-score */
    import org.apache.spark.ml.feature.StandardScaler

    val data = Seq(
      Vectors.dense(0.0, 1.0, -2.0, 3.0),
      Vectors.dense(-1.0, 2.0, 4.0, -7.0),
      Vectors.dense(13.0, 14.0, 14.0, 14.0))

    val dataFrame = sqlc.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    dataFrame.show()

    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(false)
      .setWithMean(true)

    // Compute summary statistics by fitting the StandardScaler.
    val scalerModel = scaler.fit(dataFrame)

    // Normalize each feature to have unit standard deviation.
    val scaledData = scalerModel.transform(dataFrame)
    scaledData.show()

    /** min-max */
    import org.apache.spark.ml.feature.MinMaxScaler
    val scaler2 = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures").setMax(100.0)

    // Compute summary statistics and generate MinMaxScalerModel
    val scalerModel2 = scaler2.fit(dataFrame)

    // rescale each feature to range [min, max].
    val scaledData2 = scalerModel2.transform(dataFrame)
    scaledData2.show()

  }

  def feature27() = {
    import org.apache.spark.ml.feature.ElementwiseProduct
    import org.apache.spark.mllib.linalg.Vectors

    // Create some vector data; also works for sparse vectors
    val dataFrame = sqlc.createDataFrame(Seq(
      ("a", Vectors.dense(1.0, 2.0, 3.0)),
      ("b", Vectors.dense(4.0, 5.0, 6.0)))).toDF("id", "vector")

    dataFrame.show()

    val transformingVector = Vectors.dense(0.0, 1.0, 2.0)
    val transformer = new ElementwiseProduct()
      .setScalingVec(transformingVector)
      .setInputCol("vector")
      .setOutputCol("transformedVector")

    // Batch transform the vectors to create new column:
    transformer.transform(dataFrame).show()

  }


  def feature28() = {
    import org.apache.spark.ml.feature.VectorAssembler
    import org.apache.spark.mllib.linalg.Vectors

    val dataset = sqlc.createDataFrame(
      Seq((0, Array(18, 17, 16), Vectors.dense(10.0, 0.5), Vectors.dense(0.0, 10.0, 0.5), 1.0))
    ).toDF("id", "hour", "mobile", "userFeatures", "clicked")

    dataset.show()

    val assembler = new VectorAssembler()
      .setInputCols(Array("hour", "mobile", "userFeatures"))
      .setOutputCol("features")

    val output = assembler.transform(dataset)
    println(output.select("features", "clicked").first())

  }

  def feature29() = {
    import org.apache.spark.ml.feature.QuantileDiscretizer

    val data = Array((0, 18.0), (1, 19.0), (2, 8.0), (3, 5.0), (4, 2.2))
    val df = sqlc.createDataFrame(data).toDF("id", "hour")

    df.show()

    val discretizer = new QuantileDiscretizer()
      .setInputCol("hour")
      .setOutputCol("result")
      .setNumBuckets(5)

    val result = discretizer.fit(df).transform(df)
    result.show()

  }

  def feature30() = {
    import java.util.Arrays

    import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
    import org.apache.spark.ml.feature.VectorSlicer
    import org.apache.spark.mllib.linalg.Vectors
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.StructType

    val data = Arrays.asList(Row("1", Vectors.dense(-2.0, 2.3, 0.0)))

    val defaultAttr = NumericAttribute.defaultAttr
    val attrs = Array("f1", "f2", "f3").map(defaultAttr.withName)
    val attrGroup = new AttributeGroup("userFeatures", attrs.asInstanceOf[Array[Attribute]])

    val dataset = sqlc.createDataFrame(data, StructType(Array(StructField("id", StringType), attrGroup.toStructField())))

    dataset.show()
    println(dataset.schema)

    val slicer = new VectorSlicer().setInputCol("userFeatures").setOutputCol("features")

    slicer.setIndices(Array(1)).setNames(Array("f3"))
    // or slicer.setIndices(Array(1, 2)), or slicer.setNames(Array("f2", "f3"))

    val output = slicer.transform(dataset)
    println(output.select("id", "userFeatures", "features").first())

  }

  def feature31() = {
    import org.apache.spark.ml.feature.ChiSqSelector
    import org.apache.spark.mllib.linalg.Vectors

    val data = Seq(
      (7, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1.0),
      (8, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0.0),
      (9, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0.0)
    )

    val df = sqlc.createDataFrame(data).toDF("id", "features", "clicked")

    df.show()

    val selector = new ChiSqSelector()
      .setFeaturesCol("features").setNumTopFeatures(2)
      .setLabelCol("clicked")
      .setOutputCol("selectedFeatures")

    val result = selector.fit(df).transform(df)
    result.show()

  }



  override def run(): Unit = {
    /** 1.特征提取 */
    /** 1-1 tf-idf转换 */
        feature11() //
    /** 1-2  Word2Vec */
        feature12()

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

    /** 1-13 独热编码 */
//    feature23()

    /** 1-14 低变异性数值特征转类别特征 */
//    feature24()

    /** 1-15 正则化 */
//    feature25()

    /** 1-16 标准化 */
//    feature26()

    /** 1-17 向量加权 */
//    feature27()

    /** 1-18 向量集成 */
//    feature28()

    /** 1-19 分箱 */
    feature29()

    /** 1-20 取子向量 */
//    feature30()

    /** 1-21 卡方特征选择 */
//    feature31()




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

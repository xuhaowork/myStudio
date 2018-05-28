package com.self.core.featureTransform

import com.self.core.baseApp.myAPP

object TestFeatures extends myAPP{
  override def run(): Unit = {
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


    /** one-hot编码 */
    import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}

    val df2 = sqlc.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "c")
    )).toDF("id", "category")


    val df3 = sqlc.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "d"),
      (4, "a"),
      (5, "c")
    )).toDF("someId", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
      .fit(df2)
    val indexed = indexer.transform(df2)
    println(indexer.labels.mkString(","))

    indexed.show()
    val encoder = new OneHotEncoder()
      .setInputCol("categoryIndex")
      .setOutputCol("categoryVec").setDropLast(false)
    val encoded = encoder.transform(indexed)
    encoded.select("id", "categoryVec").show()


    /** hashing-TF */







  }
}

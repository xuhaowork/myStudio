package com.self.core.polr

import com.self.core.baseApp.myAPP
import org.apache.spark.mllib.classification.polr.Polr
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object TestPolr extends myAPP{
  override def run(): Unit = {

    val lst = List(
      ("1", Array(1.0, 0.0), 1.0),
      ("1", Array(0.4, 3.0), 2.0),
      ("1", Array(1.0, 2.5), 3.0),
      ("1", Array(-1.0, 1.0), 0.0),
      ("1", Array(-1.0, 2.6), 0.0),
      ("1", Array(1.0, 2.0), 2.0),
      ("1", Array(1.2, 1.0), 2.0),
      ("1", Array(1.0, 3.3), 3.0),
      ("1", Array(-1.0, 3.0), 3.0),
      ("1", Array(-2.0, 2.0), 1.0)
    )

    val rdd: RDD[(String, LabeledPoint)] = sc.parallelize(lst).map{
      case (key, arr, label) => (key, new LabeledPoint(label, new DenseVector(arr)))
    }
    val result = new Polr().run(rdd)
    result.foreach(println)




  }
}

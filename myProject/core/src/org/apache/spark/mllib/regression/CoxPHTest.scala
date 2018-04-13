package org.apache.spark.mllib.regression

import com.self.core.baseApp.myAPP
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

/**
  * Created by dell on 2018/4/13.
  */
object CoxPHTest extends myAPP{
  override def run(): Unit = {
    val lst = List(
      Array(10, 0.5, 1, 59),
      Array(0, 0, 9, 38),
      Array(10, 0.5, 1, 99),
      Array(17, 0, 0, 16),
      Array(1, 0, 0, 79),
      Array(10, 0.5, 1, 61),
      Array(0, 0, 0, 72),
      Array(12, 5, 11, 72),
      Array(13, 0.5, 1, 49),
      Array(19, 0, 10, 72)
    ).map(_.map(_.toString.toDouble))

    val rdd: RDD[(Double, (Vector, Double))] = sc.parallelize(lst).map(arr => (arr.last.toInt, (Vectors.dense(arr.dropRight(1)), 1.0))) // (T, x, delta)的形式

    val coxPH = new CoxPH().setEpsilon(1E-2).setMaxIterations(200).setSeed(None)

    coxPH.run(rdd)







  }
}

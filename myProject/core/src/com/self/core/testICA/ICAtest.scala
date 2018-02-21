package com.self.core.testICA

import com.self.core.baseApp.myAPP
import org.apache.spark.mllib.linalg.{Matrix, Vectors}
import org.apache.spark.mllib.feature.fastICA
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD

object ICAtest extends myAPP{
  override def run(): Unit = {

    val rdd: RDD[linalg.Vector] = sc.parallelize(List.fill(10)(Array.range(1,4)
      .map(_.toDouble + scala.math.random))).map(Vectors.dense)
    val rmt = new RowMatrix(rdd)


    // step1. white the matrix.
    val whiteModel = new fastICA().whiteMatrix(rmt)
    whiteModel._1.rows.foreach(println)






  }
}

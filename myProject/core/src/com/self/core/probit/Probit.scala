package com.self.core.probit

import breeze.linalg.DenseMatrix
import com.self.core.baseApp.myAPP
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.ArrayBuffer

object Probit extends myAPP {
  override def run(): Unit = {
//    println(",")
//
//    import breeze.linalg.cholesky
//    val symMatrix = new DenseMatrix[Double](3, 3, Array(2.0, 1.0, -1.0, 1.0, 3.0, 2.0, -1.0, 2.0, 5.0))
//    val result: DenseMatrix[Double] = cholesky(symMatrix)



    val J = 3 // 3分类

    /**
      * 模型为:
      * x_i_j * beta 要求同一个x_i之间的x_i_j不能完全相同，即x_i_j = x_i_k, for all i.
      */
    val addIntercept = true


    /**
      * 进来的数据结构
      */
    import org.apache.spark.mllib.util.BLAS.dot
    import org.apache.spark.mllib.linalg.DenseVector
    val rd = new java.util.Random(123L)
    val arr = Array.tabulate(100, 3)((_, _) => rd.nextGaussian())
    val rdd = sc.parallelize(arr)
    val beta0 = new DenseVector(Array(-0.4, 1.3, 0))
    val beta1 = new DenseVector(Array(-0.4, 1.3, 5.2))
    val beta2 = new DenseVector(Array(-0.4, 1.3, -2.1))
    val newRdd = rdd.map(arr => {
      val v = new DenseVector(arr)
      val choose0 = dot(v, beta0)
      val choose1 = dot(v, beta1)
      val choose2 = dot(v, beta2)
      val choose = if(choose0 >= choose1 && choose0 >= choose2){
        0.0
      }else if(choose0 >= choose2){
        1.0
      }else{
        2.0
      }
      arr :+ choose
    })

    newRdd.take(20).foreach(x =>println(x.mkString(",")))


    val rawDataFrame: DataFrame = sqlc.createDataFrame(newRdd.map(Row.fromSeq(_)), StructType(Array(
      StructField("周围的小商店评分", DoubleType),
      StructField("距离", DoubleType),
      StructField("收入", DoubleType),
      StructField("选择商场", DoubleType)
    )))

    val publicFeatures = Array(("收入", "double"))
    val characterFeatures = ArrayBuffer(("周围的小商店评分", "double"), ("距离", "double"))

    val label = "选择商店"
    val P = (J - 1) * publicFeatures.length + characterFeatures.length

    rawDataFrame.show()







  }
}

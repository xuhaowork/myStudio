package com.self.core.ART1NeuralNet.tests

import breeze.linalg.DenseVector
import com.self.core.baseApp.myAPP
import com.self.core.ART1NeuralNet.model.ARTModel
import org.apache.spark.rdd.RDD


object ART1NetTest extends myAPP {
  val trainData = Seq(
    // 类型1 第三列为黑其余全为白
    new DenseVector[Double](Array(0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0).map(_.toDouble)),
    // 类型2 第三列和第三行为黑其余全为白
    new DenseVector[Double](Array(0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 1, 1, 1, 1, 1, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0).map(_.toDouble)),
    // 类型3 第三列和第三行加一条对角线全为黑其余全为白
    new DenseVector[Double](Array(0, 0, 1, 0, 1, 0, 0, 1, 1, 0, 1, 1, 1, 1, 1, 0, 1, 1, 0, 0, 1, 0, 1, 0, 0).map(_.toDouble)),
    // 类型4 第三列第三行加两条对角线全为黑其余全为白
    new DenseVector[Double](Array(1, 0, 1, 0, 1, 0, 1, 1, 1, 0, 1, 1, 1, 1, 1, 0, 1, 1, 1, 0, 1, 0, 1, 0, 1).map(_.toDouble))
  )


  override def run(): Unit = {
    println("this is a main file")

    val rho: Double = 0.99 // 0.1
    val numFeatures = 25

    var model = ARTModel.init(rho, numFeatures, 200)

    val rdd: RDD[DenseVector[Double]] = sc.parallelize(trainData, 4)
    if (rdd.partitions.length > 100) {
      throw new Exception("ART1神经网络是一种online learning式的学习算法, 它实质上不是以分布式的方式运行的而是逐个分区运行的, 您的分区数超过100个, 可能会严重拖慢速度")
    }


    val numForeachPartition = rdd.mapPartitionsWithIndex { case (index, iter: Iterator[DenseVector[Double]]) => Iterator((index, iter.size.toLong)) }.filter(_._2 > 0).collect()

    model.knowledge.foreach(println)



//    println("-" * 80)
//
//    numForeachPartition.foreach {
//      case (partitionId, _) =>
//        val res = rdd.sparkContext.runJob(
//          rdd,
//          (iter: Iterator[DenseVector[Double]]) => {
//            iter.foreach {
//              vec =>
//                println(s"在分区${partitionId}前, 知识为${model.knowledge.mkString(",")}")
//                println(s"分区中的数据为：${vec.data.mkString(",")}")
//                model.learn(vec)
//            }
//            model
//          },
//          Seq(partitionId)
//        )
//        model = res.head
//        println(s"经过分区${partitionId}后, 知识为${model.knowledge.mkString(",")}")
//    }
//
//    var s = 1


//    numForeachPartition.foreach {
//      case (partitionId, _) =>
//        val res = rdd.sparkContext.runJob(
//          rdd,
//          (iter: Iterator[DenseVector[Double]]) => {
//            iter.foreach {
//              vec =>
//                model.learn(vec)
//            }
//            model
//          },
//          Seq(partitionId)
//        )
//        println(s"经过分区${partitionId}后, 知识为${model.knowledge.mkString(",")}")
//    }
//    sc.runJob(rdd, (iter: Iterator[DenseVector[Double]]) => s + 1, Seq(0))





//
//    model.knowledge.foreach(println)
//
//    println("-" * 80)

    var model2 = ARTModel.init(rho, numFeatures)

    trainData.foreach {
      vec =>
        model2.learn(vec)
    }


    model.knowledge.foreach(println)



    //    val initialRecNeurons = 1
    //    val initialWMatrix: DenseMatrix[Double] = DenseMatrix.ones[Double](initialRecNeurons, numFeatures) :* (1.0 / (1 + numFeatures))
    //    val initialTMatrix: DenseMatrix[Double] = DenseMatrix.ones[Double](numFeatures, initialRecNeurons)
    //
    //    def train(vec: DenseVector[Double], initialWMatrix: DenseMatrix[Double], initialTMatrix: DenseMatrix[Double])
    //    : (Int, DenseMatrix[Double], DenseMatrix[Double]) = {
    //      var wMatrix = initialWMatrix
    //      var tMatrix = initialTMatrix
    //
    //      val recVec: DenseVector[Double] = wMatrix * vec // 此时是识别层
    //      val valueWithIndex: Array[(Double, Int)] = recVec.data.zipWithIndex
    //      val sum = vec.data.sum
    //
    //      var neurons = valueWithIndex
    //      var flag = false // 标识没有找到合适的
    //
    //      var maxIndex = -1
    //      var maxTij = DenseVector.zeros[Double](1)
    //      var similarity = Double.NaN
    //
    ////      println("-" * 80)
    ////      println("neurons:", neurons.mkString(", "))
    //      while (!neurons.isEmpty && !flag) {
    //        maxIndex = neurons.maxBy(_._1)._2
    //
    ////        println(s"识别层最大的神经元id为$maxIndex")
    //
    //        maxTij = tMatrix(::, maxIndex).toDenseVector
    //
    //        similarity = maxTij dot vec
    //
    ////        println(s"相似度为${similarity / sum}")
    //        if (similarity / sum >= rho) {
    //          flag = true
    //        } else {
    //          val drop = neurons.map(_._1).zipWithIndex.maxBy(_._1)._2
    //          neurons = neurons.slice(0, drop) ++ neurons.slice(drop + 1, neurons.length)
    //        }
    //      }
    //
    //
    //      if (flag) { // 标识找到合适的, 更新合适的权值
    ////        println("找到合适的")
    //        val vl = maxTij :* vec
    //        tMatrix(::, maxIndex) := vl
    //        wMatrix(maxIndex, ::).inner := vl :* (1 / (1 - 1.0 / numFeatures + similarity))
    //      } else { // 标识没找到合适的, 新建一个合适的
    ////        println("没有找到合适的，造一个")
    //        maxIndex = wMatrix.rows
    //        tMatrix = DenseMatrix.horzcat(tMatrix, DenseMatrix.ones[Double](numFeatures, 1))
    //        wMatrix = DenseMatrix.vertcat(wMatrix, DenseMatrix.ones[Double](1, numFeatures) :* (1.0 / (1 + numFeatures)))
    //      }
    //
    ////      println("迭代后的t矩阵为：")
    ////      println(tMatrix)
    ////      println("迭代后的w矩阵为：")
    ////      println(wMatrix)
    //      (maxIndex, wMatrix, tMatrix)
    //    }
    //
    //    var wMatrix = initialWMatrix
    //    var tMatrix = initialTMatrix


  }
}

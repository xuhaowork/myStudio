package org.apache.spark.mllib.classification

import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
  * EM算法
  * ----
  * 约定 参数封装在类型Theta中
  * 约定 x是Vector类型，对于监督学习还会有一个Double类型的标签，统一封装在PointData类中
  * 约定 z的单个元素类型是T, 一般是Double
  * ----
  * E步：
  * 估计z的后验概率，即Q(z) = p(z | x, theta), 是一个由(z: T, x: PointData, theta: Theta)
  * 到 prob: Double的映射
  * M步：
  * 最大化基于Q(z)的对数似然函数, 是由(data: RDD[Vector], Qz) 到 theta的映射
  */
class EMAlgorithm[T](
                      val maxIterations: Int,
                      val eStep: (T, PointData, Theta) => Double,
                      val mStep: (RDD[PointData], (T, PointData) => Double) => Theta,
                      val converge: (RDD[PointData], Theta, Theta) => Boolean
                    )
  extends Serializable with Logging {

  /**
    * 通过EM算法最大化对数似然函数求出参数theta
    *
    * @param data
    * @param initialWeights
    * @return
    */
  def optimize(data: RDD[PointData], initialWeights: Theta): Theta = {
    var i = 0
    var flag = false

    var previousWeights = initialWeights.copy
    var currentWeights = initialWeights.copy
    while (i < maxIterations) {
      val Qz = (z: T, x: PointData) => eStep(z, x, currentWeights)
      currentWeights = mStep(data, Qz)

      flag = converge(data, previousWeights, currentWeights)
      previousWeights = currentWeights.copy
      i += 1
    }
    currentWeights
  }


}

trait Theta extends Serializable {
  def copy: Theta
}


trait PointData extends Serializable

case class PointWithLabel(point: Vector, label: Double) extends PointData

case class PointNoneLabel(point: Vector) extends PointData

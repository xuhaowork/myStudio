package org.apache.spark.mllib.classification

import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD


class EMAlgorithm[T, U](
                         val maxIterations: Int,
                         val eStep: (T, PointData, Theta) => U,
                         val mStep: (RDD[PointData], (T, PointData) => U) => Theta,
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

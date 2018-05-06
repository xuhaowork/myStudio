package com.self.core.polr.models

import org.apache.spark.mllib.linalg.Vector

class PolrModel(val intercepts: Array[Double],
                val weights: Vector,
                val numFeatures: Int,
                val numClasses: Int
               ) extends Serializable {
  def predict(testData: Vector, byProbability: Boolean = true): String =
    if(byProbability)
      predictByProb(testData: Vector, weights, intercepts)
    else
      fit(testData: Vector).toString


  private def predictByProb(ataMatrix: Vector,
                    weightMatrix: Vector,
                    intercepts: Array[Double]): String = {
    require(ataMatrix.size == weightMatrix.size, "您输入的特征长度和系数不同")
    val probabilities = intercepts.map{case value => {
      val margin = value - dot(ataMatrix, weightMatrix)
      1.0 / (1.0 + scala.math.exp(-margin))
    }}
    val probabilitiesStart = 0.0 +: probabilities
    val probabilitiesEnd = probabilities :+ 1.0
    probabilitiesEnd.zip(probabilitiesStart).map{case (end, start) => end - start}.mkString(",")
  }

  def fit(ataMatrix: Vector): Int = {
    val probabilities = predictByProb(ataMatrix: Vector,
      weights: Vector,
      intercepts: Array[Double])
    val orderImpl = Ordering.by[(Double, Int), Double](_._1).reverse
    probabilities.split(",").map(_.trim.toDouble).zipWithIndex.sorted(orderImpl).head._2
  }


  def dot(v1: Vector, v2: Vector): Double = {
    v1.toDense.values.zip(v2.toDense.values).map{case (d1, d2) => d1 * d2}.sum
  }


}

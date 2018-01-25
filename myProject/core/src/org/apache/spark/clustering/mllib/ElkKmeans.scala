package org.apache.spark.clustering.mllib

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.util.Utils


class ElkKmeans private(private var k: Int,
                        private var maxIterations: Int,
                        private var initializationMode: String,
                        private var epsilon: Double,
                        private var seed: Long) extends Serializable {
  def this() = this(2, 20, "kmeans||", 1e-4, Utils.random.nextLong())

  def setK(k: Int): this.type = {
    this.k = k
    this
  }

  def setMaxIterations(maxIterations: Int): this.type = {
    this.maxIterations = maxIterations
    this
  }

  def setEpsilon(epsilon: Double): this.type = {
    this.epsilon = epsilon
    this
  }

  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }






}

case class VectorWithBound(vector: Vector, id: Int, l: Array[Double], u: Double, r: Boolean)

class KCenters(val centers: Array[Vector]) extends Serializable{
  val DisMap = {
    val muMap = scala.collection.mutable.Map.empty[(Int, Int), Double]
    for(i <- centers.indices){
      for(j <- 0 until i){
        muMap += (i, j) -> Vectors.sqdist(centers(i), centers(j))
      }
    }
    muMap
  }

  def disCompute(i: Int, j: Int): Double = {
    if(i < j) DisMap(j, i) else DisMap(i, j)
  }


}
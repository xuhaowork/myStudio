package org.apache.spark.mllib.classification

/**
  * 广义线性模型
  */

import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.BLAS.{axpy, dot}
import org.apache.spark.mllib.optimization.Gradient
import org.apache.spark.mllib.util.MLUtils
import breeze.stats.distributions.Gaussian
import org.apache.spark.mllib.linalg.{Vector, Vectors}

class ProbitGradient(numClasses: Int) extends Gradient {
  require(numClasses == 2, "目前只支持二分类")

  def this() = this(2)

  override def compute(data: Vector, label: Double, weights: Vector): (Vector, Double) = {
    val gradient = Vectors.zeros(weights.size)
    val loss = compute(data, label, weights, gradient)
    (gradient, loss)
  }


  override def compute(data: linalg.Vector,
                       label: Double,
                       weights: linalg.Vector,
                       cumGradient: linalg.Vector): Double = {
    val dataSize = data.size

    // (weights.size / dataSize + 1) is number of classes
    require(weights.size % dataSize == 0 && numClasses == weights.size / dataSize + 1)

    /**
      * For Binary Logistic Regression.
      *
      * Although the loss and gradient calculation for multinomial one is more generalized,
      * and multinomial one can also be used in binary case, we still implement a specialized
      * binary version for performance reason.
      */
    val q  = 2 * label - 1.0
    val margin = 1.0 * dot(data, weights)
    val qMargin: Double = q * margin
    val gaussian = new Gaussian(0.0, 1.0)
    val multiplier = - q * gaussian.unnormalizedPdf(qMargin) / gaussian.cdf(multiplier)

    axpy(multiplier, data, cumGradient)

    if (label > 0) {
      // The following is equivalent to log(1 + exp(margin)) but more numerically stable.
      MLUtils.log1pExp(margin)
    } else {
      MLUtils.log1pExp(margin) - margin
    }
  }
}


class GMMWithLBFGS {

}

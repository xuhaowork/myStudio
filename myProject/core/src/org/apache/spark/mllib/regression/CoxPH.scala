package org.apache.spark.mllib.regression

import breeze.linalg.DenseVector
import org.apache.spark.mllib.linalg.BLAS.{axpy, dot, scal}
import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.rdd.RDD

/**
  * Created by dell on 2018/4/13.
  */
class CoxPH(private var epsilon: Double,
            private var maxIterations: Int,
            private var learningRate: Double,
            private var sampleFraction: Double,
            private var seed: Long
           ) extends Serializable{
  def this() = this(1E-4, 200, 0.9, 0.7, 123L)

  /**
    * 最终的收敛条件
    * @param epsilon 当beta迭代差的二阶范式小于epsilon时终止
    * @return
    */
  def setEpsilon(epsilon: Double): this.type = {
    this.epsilon = epsilon
    this
  }

  /**
    * 最大迭代次数
    * @param maxIterations 梯度下降中当迭代大于等于maxIterations终止训练
    * @return
    */
  def setMaxIterations(maxIterations: Int): this.type = {
    this.maxIterations = maxIterations
    this
  }

  /**
    * 学习率
    * @param learningRate 梯度下降的学习率
    * @return
    */
  def setLearningRate(learningRate: Double): this.type = {
    this.learningRate = learningRate
    this
  }

  /**
    * 抽样比率
    * @param sampleFraction 随机批次梯度下降中每次抽取的比率
    * @return
    */
  def setSampleFraction(sampleFraction: Double): this.type = {
    this.sampleFraction = sampleFraction
    this
  }

  /**
    * 随机数种子
    * @param seed 随机数种子，如果seed为None则随机数种子为完全随机数
    * @return
    */
  def setSeed(seed: Option[Long]): this.type = {
    val rd = new java.util.Random()
    this.seed = seed match {
      case Some(x) => x
      case None => rd.nextLong()
    }
    this
  }


  /**
    * 求偏似然函数的梯度
    * ----
    * @param rdd 输入的数据，变量依次为[生存期限, (自变量, 删失)]，未删失未1.0，删失为0.0
    * @param beta 回归系数
    * @return 偏似然函数的梯度
    */
  private def partialLikelihoodGradient(rdd: RDD[(Double, (Vector, Double))], beta: Vector): Vector = {
    /**
      * 1)求自变量向量的和
      */
    val sum: Vector = rdd.values.map {
      case (x, delta) =>
        scal(delta, x)
        x
    }.reduce {
      case (x1, x2) =>
        axpy(1.0, x1, x2)
        x2
    }
    /**
      * 2)根据t_i分组求rate(delta, x, beta, c(t_i))和multiplier(x, beta, c(t_i))
      */
    val primitiveRdd = rdd.mapValues {
      case (x, delta) => {
        val expXMultiplyBeta = scala.math.exp(dot(x, beta))
        scal(expXMultiplyBeta * delta, x)
        (x, expXMultiplyBeta)
      }
    }.reduceByKey {
      case ((x1, expXMultiplyBeta1), (x2, expXMultiplyBeta2)) => {
        axpy(1.0, x1, x2)
        (x2, expXMultiplyBeta1 + expXMultiplyBeta2)
      }
    }
    /**
      * 3)膨胀并分组求和
      * 膨胀为要求和的倒三角形 => 对每个t_i求出sum[rate(delta, x, beta, c(t_j)) | t_j >= t_i]
      * 和sum[multiplier(x, beta, c(t_i)) | t_j >= t_i]
      */
    val inflateRdd = primitiveRdd.keys.cartesian(primitiveRdd)
      .filter {
        case (t_i, (t_j, (_, _))) => t_j >= t_i
      }.mapValues {
      case (_, (rate_j, multiplier_j)) => (rate_j, multiplier_j)
    }.reduceByKey {
      case ((rate_i, multiplier_i), (rate_j, multiplier_j)) => {
        axpy(1.0, rate_i, rate_j)
        (rate_j, multiplier_i + multiplier_j)
      }
    }
    /**
      * 4)求出最终的加权自变量向量
      * 根据膨胀的三角形中每个t_i对应的sum[rate(delta, x, beta, c(t_j)) | t_j >= t_i]以及
      * sum[multiplier(x, beta, c(t_i)) | t_j >= t_i]，将sum[rate(delta, x, beta, c(t_j)) | t_j >= t_i]
      * 每个元素乘以sum[multiplier(x, beta, c(t_i)) | t_j >= t_i]的倒数，并对所有的t_i对应的值求和
      */
    val expWeightedSum: Vector = inflateRdd.mapValues {
      case (sumRate, multiplier) =>
        scal(1 / multiplier, sumRate)
        sumRate
    }.values.reduce {
      case (sumRate1, sumRate2) =>
        axpy(1.0, sumRate1, sumRate2)
        sumRate2
    }
    /**
      * 5)求出最终的偏似然梯度
      * 最后将1)与4)做差，求出偏似然的梯度
      */
    axpy(-1.0, expWeightedSum, sum)
    sum
  }


  /**
    * 通过梯度下降求Cox-PH模型的回归系数
    * @param data 输入的数据
    */
  def run(data: RDD[(Double, (Vector, Double))]): Vector = {
    val epsilon = this.epsilon
    val maxIterations = this.maxIterations
    val fraction = this.sampleFraction
    val learningRate = this.learningRate
    var i = 0
    var error = epsilon + 1

    val rd = new java.util.Random(seed)
    val firstVector = data.take(1).head._2._1
    val vectorLength = firstVector.toArray.length

    val beta: Vector = Vectors.dense(Array.tabulate(vectorLength)(_ => rd.nextDouble() / 100))


    while(i < maxIterations && error >= epsilon){
      val sampleData = data.sample(false, fraction, seed)
      val gradient = partialLikelihoodGradient(sampleData, beta)
      val oldBeta = beta.copy
      println(s"-----$i-----")
      println("oldBeta" + oldBeta)
      axpy(-1 * learningRate, gradient, beta)
      println("beta" + beta)
      axpy(-1.0, beta, oldBeta)
      println("diff" + oldBeta)
      error = Vectors.norm(oldBeta, 2.0)
      println(error)
      i += 1
    }

    beta
  }




  def alphaJ(data: RDD[(Double, (Vector, Double))], beta: Vector) = {
    val timeGroup = data.mapValues{case (x, alpha) => math.exp(dot(x, beta))/(1 - math.exp(math.log(alpha)*math.exp(dot(x, beta))))}.reduceByKey{
      case (dot1, dot2) => dot1 + dot2
    }

  }



}

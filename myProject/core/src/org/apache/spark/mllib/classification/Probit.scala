package org.apache.spark.mllib.classification

/**
  * probit回归的核心运算API
  */

/**
  * editor: xuhao
  * date: 2018-05-15 10:30:00
  */

import breeze.stats.distributions.Gaussian
import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.classification.impl.GLMClassificationModel
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.BLAS.{axpy, dot}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.optimization._
import org.apache.spark.mllib.pmml.PMMLExportable
import org.apache.spark.mllib.regression.{GeneralizedLinearAlgorithm, GeneralizedLinearModel, LabeledPoint}
import org.apache.spark.mllib.util.{DataValidators, Saveable}
import org.apache.spark.rdd.RDD

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

    numClasses match {
      case 2 =>
        // (weights.size / dataSize + 1) is number of classes
        require(weights.size % dataSize == 0 && numClasses == weights.size / dataSize + 1)

        val q = 2 * label - 1.0
        val margin = dot(data, weights)
        val qMargin: Double = q * margin
        val gaussian = new Gaussian(0.0, 1.0)
        val multiplier = -q * gaussian.unnormalizedPdf(qMargin) / gaussian.cdf(qMargin)

        axpy(multiplier, data, cumGradient)

        val cdfMargin = gaussian.cdf(margin)
        if (label > 0) {
          if (cdfMargin <= 0.0)
            Double.MinValue
          else
            scala.math.log(cdfMargin)
        } else {
          if (cdfMargin >= 1.0)
            Double.MinValue
          else
            scala.math.log(1 - cdfMargin)
        }

      case num: Int => {
//        import com.zzjz.deepinsight.core.polr.models.VectorImplicit.{VectorLastImplicit, VectorDropImplicit}
        val halfL = weights.last(num * (num - 1) / 2) // 取出rho
        // 生成下三角矩阵，避免无法识别，需要L11 = 1, L_{j, 1} = 0
        // cholesky分解
        // 通过MCMC求近似似然
        // 数值求导


      }


    }


  }
}


class ProbitRegressionWithSGD private[mllib](
                                              private var stepSize: Double,
                                              private var numIterations: Int,
                                              private var regParam: Double,
                                              private var miniBatchFraction: Double)
  extends GeneralizedLinearAlgorithm[ProbitRegressionModel] with Serializable {

  this.setFeatureScaling(true)

  override val optimizer: GradientDescent =
    new GradientDescent(new ProbitGradient, new SquaredL2Updater)
      .setStepSize(stepSize)
      .setNumIterations(numIterations)
      .setRegParam(regParam)
      .setMiniBatchFraction(miniBatchFraction)

  override protected val validators = List(multiLabelValidator)

  private def multiLabelValidator: RDD[LabeledPoint] => Boolean = { data =>
    if (numOfLinearPredictor > 1) {
      DataValidators.multiLabelValidator(numOfLinearPredictor + 1)(data)
    } else {
      DataValidators.binaryLabelValidator(data)
    }
  }

  override protected def createModel(weights: Vector, intercept: Double): ProbitRegressionModel = {
    new ProbitRegressionModel(weights, intercept, numFeatures, numOfLinearPredictor + 1)
  }
}


class ProbitRegressionWithLBFGS extends GeneralizedLinearAlgorithm[ProbitRegressionModel]
  with Serializable {

  this.setFeatureScaling(true)

  override val optimizer = new LBFGS(new ProbitGradient, new SquaredL2Updater)

  override protected val validators = List(multiLabelValidator)

  private def multiLabelValidator: RDD[LabeledPoint] => Boolean = { data =>
    if (numOfLinearPredictor > 1) {
      DataValidators.multiLabelValidator(numOfLinearPredictor + 1)(data)
    } else {
      DataValidators.binaryLabelValidator(data)
    }
  }

  override protected def createModel(weights: Vector, intercept: Double)
  : ProbitRegressionModel = {
    new ProbitRegressionModel(weights, intercept, numFeatures, numOfLinearPredictor + 1)
  }
}


class ProbitRegressionModel(
                             override val weights: Vector,
                             override val intercept: Double,
                             val numFeatures: Int,
                             val numClasses: Int)
  extends GeneralizedLinearModel(weights, intercept) with ClassificationModel with Serializable
    with Saveable with PMMLExportable {

  if (numClasses == 2) {
    require(weights.size == numFeatures, "不含截距项的系数长度应该和特征数目一致")
  } else {
    throw new Exception("暂时没有实现多元probit回归。")
  }


  /**
    * Constructs a [[LogisticRegressionModel]] with weights and intercept for binary classification.
    */
  def this(weights: Vector, intercept: Double) = this(weights, intercept, weights.size, 2)

  private var threshold: Option[Double] = Some(0.5)

  def setThreshold(threshold: Double): this.type = {
    this.threshold = Some(threshold)
    this
  }

  def getThreshold: Option[Double] = threshold

  def clearThreshold(): this.type = {
    threshold = None
    this
  }

  override protected def predictPoint(
                                       dataMatrix: Vector,
                                       weightMatrix: Vector,
                                       intercept: Double): Double = {
    require(dataMatrix.size == numFeatures)

    // If dataMatrix and weightMatrix have the same dimension, it's binary logistic regression.
    if (numClasses == 2) {
      val score = new Gaussian(0.0, 1.0).cdf(dot(weightMatrix, dataMatrix) + intercept)
      threshold match {
        case Some(t) => if (score > t) 1.0 else 0.0
        case None => score
      }
    } else {
      throw new Exception("暂时没有实现多元probit回归。")
    }
  }

  @Since("1.3.0")
  override def save(sc: SparkContext, path: String): Unit = {
    GLMClassificationModel.SaveLoadV1_0.save(sc, path, this.getClass.getName,
      numFeatures, numClasses, weights, intercept, threshold)
  }

  override protected def formatVersion: String = "1.0"

  override def toString: String = {
    s"${super.toString}, numClasses = $numClasses, threshold = ${threshold.getOrElse("None")}"
  }
}

object Probit {
  /**
    *
    * @param input             输入的数据
    * @param numIterations     梯度下降的最大迭代次数
    * @param stepSize          梯度下降需要的学习率
    * @param miniBatchFraction SGD中需要的初始权重
    * @param initialWeights    初始值，需要和对应的长度一致
    * @param addIntercept      是否加入截距项
    * @return 得到的ProbitRegressionModel
    */
  def trainWithSGD(
                    input: RDD[LabeledPoint],
                    numIterations: Int,
                    stepSize: Double,
                    miniBatchFraction: Double,
                    initialWeights: Vector,
                    addIntercept: Boolean): ProbitRegressionModel = {
    new ProbitRegressionWithSGD(stepSize, numIterations, 0.0, miniBatchFraction)
      .setIntercept(addIntercept)
      .run(input, initialWeights)
  }


  /**
    *
    * @param input             输入的数据
    * @param numIterations     梯度下降的最大迭代次数
    * @param stepSize          梯度下降需要的学习率
    * @param miniBatchFraction SGD中需要的初始权重
    * @return 得到的ProbitRegressionModel
    */
  def trainWithSGD(
                    input: RDD[LabeledPoint],
                    numIterations: Int,
                    stepSize: Double,
                    miniBatchFraction: Double,
                    intercept: Boolean): ProbitRegressionModel = {
    new ProbitRegressionWithSGD(stepSize, numIterations, 0.0, miniBatchFraction)
      .setIntercept(intercept)
      .run(input)
  }


  def trainWithSGD(
                    input: RDD[LabeledPoint],
                    numIterations: Int,
                    stepSize: Double): ProbitRegressionModel = {
    trainWithSGD(input, numIterations, stepSize, 1.0, true)
  }

  def trainWithSGD(
                    input: RDD[LabeledPoint],
                    numIterations: Int
                  ): ProbitRegressionModel = {
    trainWithSGD(input, numIterations, 1.0)
  }


  def trainWithLBFGS(input: RDD[LabeledPoint],
                     addIntercept: Boolean): ProbitRegressionModel = {
    new ProbitRegressionWithLBFGS()
      .setIntercept(addIntercept)
      .run(input)
  }

  def trainWithLBFGS(input: RDD[LabeledPoint]): ProbitRegressionModel = {
    new ProbitRegressionWithLBFGS()
      .setIntercept(true)
      .run(input)
  }


}




package org.apache.spark.mllib.classification.polr

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.BLAS.dot
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

class Polr(levels: Int,
                         numIterations: Int,
                         stepSize: Double,
                         miniBatchFraction: Double) extends Serializable {
  def this() = this(4, 200, 0.3, 0.3)

  def run(input: RDD[(String, LabeledPoint)]) = {
    /**
      * 特征转化
      */
    val transformRdd = input.flatMapValues(transform(_, levels))
    val logitModel = new LogisticRegressionWithSGD(stepSize,
      numIterations, 0.0, miniBatchFraction)
      .setIntercept(true).run(transformRdd.values)
    import VectorImplicit.VectorLastmplicit
    val interceptLst = logitModel.intercept +: logitModel.weights.last(levels - 2)

    println(logitModel.intercept)

    println("stage1:", interceptLst.mkString(","))

    import ArrayUtilsImplicit.ArrayCumsum
    val multiIntercept = interceptLst.cumsum

    println("stage2", multiIntercept.mkString(","))

    import VectorImplicit.VectorDropImplicit
    val weight = logitModel.weights.dropRight(levels - 2)

    println("stage3", weight)

    input.mapValues(labeledPoint => {
      val p = multiIntercept.map(intercept => {
        val margin = -intercept + dot(weight, labeledPoint.features)
        1.0 / (1.0 + scala.math.exp(-margin))
      })
      (labeledPoint, p.mkString(","))
    })
  }


  private def transform(labeledPoint: LabeledPoint, levels: Int): Array[LabeledPoint] = {
    import VectorImplicit.VectorAppendImplicit
    // k is the levels
    Array.tabulate(levels - 1)(k => {
      if(labeledPoint.label > k) {
        val newFeatures = labeledPoint.features.append(
          Array.tabulate(levels - 2)(i => if(i < k) 1.0 else 0.0)
        )
        LabeledPoint(1.0, newFeatures)
      } else {
        val newFeatures = labeledPoint.features.append(
          Array.tabulate(levels - 2)(i => if(i < k) 1.0 else 0.0)
        )
        LabeledPoint(0.0, newFeatures)
      }
    })
  }





}


object Polr {
  /**
    *
    * @param input 级别从小到大依次为0, 1, 2, ..., levels - 1
    * @param levels 级别数
    * @param numIterations 最大迭代次数
    * @param stepSize 梯度下降的步长
    * @param miniBatchFraction 迭代批次的比率
    * @param initialWeights 初始权重
    * @return
    */
  def train(
             input: RDD[LabeledPoint],
             levels: Int,
             numIterations: Int,
             stepSize: Double,
             miniBatchFraction: Double,
             initialWeights: Vector) = {
  }

}

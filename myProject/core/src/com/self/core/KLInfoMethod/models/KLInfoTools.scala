package com.self.core.KLInfoMethod.models

import org.apache.spark.rdd.RDD


/** 求和的中间结果 */
case class KLMeta(refResult: Double, varResult: Array[Double]) {
  def +(other: KLMeta): KLMeta = {
    KLMeta(refResult + other.refResult, varResult.zip(other.varResult).map { case (v1, v2) => v1 + v2 })
  }

  def *(multiplier: Double): KLMeta = {
    KLMeta(refResult * multiplier, varResult.map(_ * multiplier))
  }

}

object KLInfoTools {
  /** 一次性求出所有的和 */
  def getSum(rdd: RDD[KLMeta], arrayLength: Int, range: Range): Map[Int, KLMeta] = {
    val count = rdd.count()

    val zero: Map[Int, KLMeta] = range.map(i => (i, KLMeta(0.0, new Array[Double](arrayLength)))).toMap

    def seqOp(sumMap: Map[Int, KLMeta], tup: (Long, KLMeta)): Map[Int, KLMeta] = {
      sumMap.map {
        case (mv, result1) =>
          val multiplier = if (mv >= 0) {
            if (tup._1 - mv > 0) 1.0 else 0.0
          } else {
            if (tup._1 - count - mv >= 0) 1.0 else 0.0
          }
          (mv, result1 + tup._2 * multiplier)
      }
    }

    def comOp(sumMap1: Map[Int, KLMeta], sumMap2: Map[Int, KLMeta]): Map[Int, KLMeta] =
      sumMap1.map {
        case (key, value) =>
          val result = if (sumMap2.contains(key))
            value + sumMap2(key)
          else
            value
          (key, result)
      }

    rdd.zipWithIndex.map {
      case (value, idx) =>
        (idx, value)
    }.aggregate(zero)(seqOp, comOp)
  }


}

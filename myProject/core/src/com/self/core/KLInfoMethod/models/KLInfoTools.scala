package com.self.core.KLInfoMethod.models

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer


/** 求和的中间结果 */
case class KLMeta(refResult: Double, varResult: Array[Double]) extends Serializable {
  def +(other: KLMeta): KLMeta = {
    KLMeta(refResult + other.refResult, varResult.zip(other.varResult).map { case (v1, v2) => v1 + v2 })
  }

  def *(multiplier: Double): KLMeta = {
    KLMeta(refResult * multiplier, varResult.map(_ * multiplier))
  }

  def /(other: KLMeta): KLMeta = {
    val ref = if(other.refResult == 0.0 || other.refResult.isNaN)
      refResult
    else
      refResult / other.refResult
    val variables = varResult.zip(other.varResult).map {
      case (v1, v2) => if(v2 == 0.0 || v2.isNaN) v1 else v1 / v2
    }
    KLMeta(ref, variables)
  }

  def KL: Array[Double] = {
    require(refResult >= 0.0 && refResult <= 1.0, s"您输入的${refResult}不满足概率条件")

    varResult.map { variable =>
      require(variable >= 0.0 && variable <= 1.0, s"您输入的${variable}不满足概率条件")
      if(refResult == 0.0)
        0.0
      else if(variable == 0.0)
        Double.MaxValue
      else
        refResult * scala.math.log(refResult / variable)
    }
  }
}

/** 用于存储所有分区内先期滞后信息的类 */
case class LagInfoForPartition(headerElements: Array[Double], tailElements: Array[Double]) extends Serializable {
  def getHeaders(num: Int): Array[Double] = {
    headerElements.take(num)
  }

  def getTails(num: Int): Array[Double] = {
    tailElements.takeRight(num)
  }
}


/** 用于存储所有分区内先期滞后信息的类 */
class LagInfo(
               val lagInfoForPartition: collection.Map[Int, LagInfoForPartition],
               val numsForPartitions: collection.Map[Int, Int],
               val rangeForTail: Int,
               val rangeForHead: Int) extends Serializable {
  /** 分区id -> 元素个数 */


  /** 获取分区partition后面分区的前minRange个元素 */
  def headKAfterPartitionP(p: Int): Array[Double] = {
    require(lagInfoForPartition.keySet.contains(p), s"${p}不在分区id中")
    require(p <= lastPartitionNumCanLag, s"您的分区id:${p}之后没有${rangeForHead}条数据")

    val result = ArrayBuffer.empty[Double]
    var partition = p + 1
    var num = 0
    if (lagInfoForPartition.get(partition).isEmpty)
      throw new Exception(s"未能找到分区${partition}中的元素")
    else {
      while (num < rangeForTail) {
        val takeN = lagInfoForPartition(partition).headerElements.take(rangeForTail - num)
        num += takeN.length
        partition += 1
        result ++= takeN
      }
    }
    result.toArray
  }

  /** 获取分区p之前分区的最后rangeForTail个元素 */
  def tailKBeforePartitionP(p: Int): Array[Double] = {
    require(lagInfoForPartition.keySet.contains(p), s"${p}不在分区id中")
    require(p >= leastPartitionNumCanLead, s"您的分区id:${p}之前没有${rangeForTail}条数据")

    var result = Array.empty[Double]
    var partition = p - 1
    var num = 0
    if (lagInfoForPartition.get(partition).isEmpty)
      throw new Exception(s"未能找到分区${partition}中的元素")
    else {
      while (num < rangeForTail) {
        val takeN = lagInfoForPartition(partition).headerElements.takeRight(rangeForTail - num)
        num += takeN.length
        partition -= 1
        result = takeN ++ result
      }
    }
    result
  }

  val leastPartitionNumCanLead: Int = LeastPartitionNumCanLead

  val lastPartitionNumCanLag: Int = LastPartitionNumCanLag


  def LeastPartitionNumCanLead: Int = {
//    var i = 1 // 第一个分区肯定无法取前一个分区的最后k个元素
//    var flag = 0
//    while (rangeForHead > flag && (numsForPartitions.keySet contains i )) {
//      flag += numsForPartitions.apply(i)
//      i += 1
//    }
//    if(rangeForHead > flag)
//      throw new Exception(s"遍历了所有分区数据仍然为${flag}条, 最后的分区id为${i}")
//    i

    numsForPartitions.toArray
      .sortBy(_._1)
      .scan((0, 0))((a, b) => (b._1, a._2 + b._2))
      .drop(1)
      .filter(a => a._2 >= rangeForHead)
      .map(_._1)
      .min + 1
  }

  def LastPartitionNumCanLag: Int = {
//    var i = numsForPartitions.keySet.max // 最后一个分区肯定无法取下一个分区的前k个元素
//    numsForPartitions.toArray.sortBy(_._1)
//    var flag = 0
//    while (-rangeForTail > flag) {
//      require(numsForPartitions.keySet contains i, s"分区id${i}不在分区id中")
//      flag += numsForPartitions.getOrElse(i, 0)
//      i -= 1
//    }
//    i

    numsForPartitions.toArray
      .sortBy(_._1)
      .scanRight(0, 0)((a, b) => (a._1, a._2 + b._2))
      .drop(1)
      .filter(a => a._2 >= rangeForTail)
      .map(_._1)
      .max - 1

  }


}


object KLInfoTools extends Serializable {
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
            if (tup._1 - count - mv <= 0) 1.0 else 0.0
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

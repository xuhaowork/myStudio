package com.self.core.generalTimeBinner.tools

import com.self.core.generalTimeBinner.models.{AbsTimeMeta, BinningTime, RelativeMeta, TimeMeta}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.catalyst.util.GenericArrayData

object Utils extends Serializable {
  val unitMatch: Map[String, Long] = Map(
    "year" -> 3600L * 24L * 365L * 1000L, // 约数
    "month" -> 3600L * 24L * 30L * 1000L, // 约数
    "week" -> 3600L * 24L * 7L * 1000L,
    "day" -> 3600L * 24L * 1000L,
    "hour" -> 3600L * 1000L,
    "minute" -> 60L * 1000L,
    "second" -> 1000L,
    "millisecond" -> 1L
  )

  val adaptiveTimeFormat4Unit: Map[String, String] = Map(
    "year" -> "yyyy年MM月dd日 HH:mm:ss", // 约数
    "month" -> "MM月dd日 HH:mm:ss", // 约数
    "week" -> "MM月dd日 HH:mm:ss",
    "day" -> "dd日 HH:mm:ss",
    "hour" -> "HH:mm:ss",
    "minute" -> "HH:mm:ss",
    "second" -> "HH:mm:ss",
    "millisecond" -> "HH:mm:ss.SSS"
  )


  def adaptTimeFormat(window: Long): String = {
    var timeFormat = ""
    val unitCollection = Iterator("year", "month", "week", "day", "hour", "minute", "second", "millisecond")
    var findAGirl = false
    var grade = 0
    while (unitCollection.hasNext && !findAGirl) {
      val nextUnit = unitCollection.next()
      if (window >= unitMatch(nextUnit)) {
        timeFormat = adaptiveTimeFormat4Unit(nextUnit)
        findAGirl = true
      }
      grade += 1
    }
    timeFormat
  }


  def reConstruct(arr: Array[(TimeMeta, Int, Int, Int)]): BinningTime = {
    val values: Map[(Int, Int), (TimeMeta, Int)] = arr.map {
      case (element, dpth, pstn, parentPosition) =>
        ((dpth, pstn), (element, parentPosition))
    }.toMap
    val deep = arr.map(_._2).max
    Utils.constructBinningTimeFromArray(values, deep, -1)
  }


  /** 搞定改成的两个节点形成一个树，直至根节点 */
  def constructBinningTimeFromArray(values: Map[(Int, Int), (TimeMeta, Int)], deep: Int, position: Int): BinningTime = {
    if (deep == 0) { // 根节点时终止
      println(deep + "时终止",
        new BinningTime(values(deep, position)._1, None, None, true, deep, position, values(deep, position)._2))
      new BinningTime(values(deep, position)._1, None, None, true, deep, position, values(deep, position)._2)
    } else {
      val tree = constructBinningTimeFromArray(
        values, deep - 1, values(deep, position)._2)
      val bt = tree.getTipNode(values(deep, position)._2 == -1) // 获得整个树的最后分支
      println(deep + "时" + "获得父节点" + bt)

      if (position < 0) { // 左子树, 此时dpth必然大于0
        bt.setLeftChild(
          new BinningTime(values(deep, position)._1,
            None, None, true, deep, position, values(deep, position)._2))
        bt.setRightChild(
          new BinningTime(values(deep, position * -1)._1,
            None, None, true, deep, position, values(deep, position * -1)._2))
      } else {
        bt.setRightChild(
          new BinningTime(values(deep, position)._1,
            None, None, true, deep, position, values(deep, position)._2))
        bt.setLeftChild(
          new BinningTime(values(deep, position * -1)._1,
            None, None, true, deep, position, values(deep, position * -1)._2))
      }
      println("经过组装", bt)
      tree
    }
  }



  def serializeForTimeMeta(obj: Any): InternalRow = obj match {
    case absMT: AbsTimeMeta =>
      val row = new GenericMutableRow(4)
      row.setByte(0, 0)
      row.setLong(1, absMT.value)
      if (absMT.rightBound.isDefined)
        row.setLong(2, absMT.rightBound.get)
      else
        row.setNullAt(2)
      absMT.timeFormat.map(char => char.toInt).toArray
      row.update(3, new GenericArrayData(absMT.timeFormat.map(char => char.toInt).toArray))
      row
    case rlMT: RelativeMeta =>
      val arr = rlMT.timeFormat.map(char => char)
      val row = new GenericMutableRow(3 + arr.length)
      row.setByte(0, 1)
      row.setLong(1, rlMT.value)
      if (rlMT.rightBound.isDefined)
        row.setLong(2, rlMT.rightBound.get)
      else
        row.setNullAt(2)
      row.update(3, new GenericArrayData(rlMT.timeFormat.map(char => char.toInt).toArray))
      row
    case _ =>
      throw new Exception("您输入类型不是TimeMeta在spark sql中没有预定义的序列化方法")
  }

  def deserializeForTimeMeta(datum: Any): TimeMeta = {
    datum match {
      case row: InternalRow =>
        val mtType = row.getByte(0)
        val value = row.getLong(1)
        val rightBound = if (row.isNullAt(2)) None else Some(row.getLong(2))
        val timeFormat = row.getArray(3).toIntArray().map(_.toChar).mkString("")
        if (mtType == 0) {
          new AbsTimeMeta(value, rightBound, timeFormat)
        } else {
          new RelativeMeta(value, rightBound, timeFormat)
        }
    }
  }


}

/**
  *
  * @param timeFormat 时间格式
  * @param grade      分箱对应单位的级别
  *                   当分箱长度大于等于1年时，级别为0，展示时间格式"yyyy年MM月dd日 HH:mm:ss"
  *                   当分箱长度小于1年大于等于1月时，级别为1，展示时间格式为"MM月dd日 HH:mm:ss"
  *                   当分箱长度小于1月大于等于1星期时，级别为2，展示时间格式为"MM月dd日 HH:mm:ss"
  *                   当分箱长度小于1星期大于等于1天时，级别为3，展示时间格式为"dd日 HH:mm:ss"
  *                   当分箱长度小于1天大于等于1小时时，级别为4，展示时间格式为"HH:mm:ss",
  *                   当分箱长度小于1小时大于等于1分钟时，级别为5，展示时间格式为"HH:mm:ss",
  *                   当分箱长度小于1分钟大于等于1秒钟时，级别为6，展示时间格式为"HH:mm:ss",
  *                   当分箱长度小于1秒钟大于等于1毫秒时，级别为7，展示时间格式为"HH:mm:ss.SSS"
  */
case class AdaptiveTimeFormat(timeFormat: String, grade: Int)

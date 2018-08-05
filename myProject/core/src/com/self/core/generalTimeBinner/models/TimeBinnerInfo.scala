package com.self.core.generalTimeBinner.models

import org.joda.time.DateTime

/**
  * 分箱信息
  * ----
  *
  * @param phase  一个初始相位
  * @param window 分箱窗宽
  * @param unit   单位
  *               year
  *               month
  *               week
  *               day
  *               hour
  *               minute
  *               second
  *               millisecond
  * @param left   如果是非叶节点的话, 对BinningTime的左节点分箱还是右结点分箱
  */
class TimeBinnerInfo(
                      val phase: DateTime,
                      val window: Long,
                      val unit: String,
                      val left: Boolean = true
                    ) extends Serializable {


}


import org.apache.spark.sql.types.{DataType, LongType, StringType, TimestampType}

/**
  * 时间列信息
  * ----
  *
  * @param timeColName 时间列名
  * @define timeColType 时间列类型
  *                     目前只支持 String类型、Long类型、Timestamp类型、BinningTimeUDT类型
  */
abstract class TimeColInfo(val timeColName: String) extends Serializable {
  val timeColType: DataType
}

/**
  * StringType类型列对应的时间信息
  * ----
  *
  * @param timeColName 时间列名
  * @param timeFormat  时间字符串的格式
  */
class StringTypeTimeColInfo(override val timeColName: String,
                            val timeFormat: String) extends TimeColInfo(timeColName) {
  val timeColType: DataType = StringType
}

/**
  * LongType类型列对应的时间信息
  * ----
  *
  * @param timeColName   时间列名
  * @param timeStampUnit 时间戳单位 只能是秒或毫秒
  */
class LongTypeTimeColInfo(override val timeColName: String,
                          val timeStampUnit: String) extends TimeColInfo(timeColName) {
  val timeColType: DataType = LongType

}

/**
  * TimestampType类型列对应的时间信息
  * ----
  *
  * @param timeColName 时间列名
  */
class TimestampTypeTimeColInfo(override val timeColName: String) extends TimeColInfo(timeColName) {
  val timeColType: DataType = TimestampType
}



/**
  * BinningTimeUDT类型列对应的时间信息
  * ----
  *
  * @param timeColName 时间列名
  */
class BinningTypeTimeColInfo(override val timeColName: String, val forLeft: Boolean) extends TimeColInfo(timeColName) {
  val timeColType: DataType = TimestampType
}





package com.self.core.generalTimeBinning.models

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

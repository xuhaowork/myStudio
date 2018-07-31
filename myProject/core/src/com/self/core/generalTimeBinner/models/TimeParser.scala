package com.self.core.generalTimeBinner.models

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.SparkException


/**
  * 解析器
  * ----
  * 将时间记录解析为分箱时间
  *
  * @param timeColInfo 时间列信息
  */
class TimeParser(val timeColInfo: TimeColInfo) extends Serializable {
  def parse(time: String): BinningTime = {
    timeColInfo match {
      case st: StringTypeTimeColInfo =>
        val timeFormat = st.timeFormat
        val meta = if (timeFormat.toLowerCase contains "yyyy") {
          try {
            new AbsTimeMeta(new SimpleDateFormat(timeFormat).parse(time).getTime, timeFormat)
          } catch {
            case e: Exception =>
              throw new SparkException(s"在解析string类型${timeFormat}格式的时间字符串是出现错误，数据为$time, " +
                s"具体信息${e.getMessage}")
          }
        } else {
          new RelativeMeta(new SimpleDateFormat(timeFormat).parse(time).getTime, timeFormat)
        }
        new BinningTime(meta)

      case _: LongTypeTimeColInfo =>
        throw new SparkException("数据为string类型，但您输入的时间列信息为long类型")
      case _: TimestampTypeTimeColInfo =>
        throw new SparkException("数据为string类型，但您输入的时间列信息为timestamp类型")
    }
  }

  def parse(time: Long): BinningTime = {
    timeColInfo match {
      case _: StringTypeTimeColInfo =>
        throw new SparkException("数据为long类型，但您输入的时间列信息为string类型")
      case lt: LongTypeTimeColInfo =>
        val unit = lt.timeStampUnit
        try {
          new BinningTime(new AbsTimeMeta(if (unit == "millisecond") time else time * 1000))
        } catch {
          case e: Exception =>
            throw new SparkException(s"在将long格式的数据${time}转为时间格式时出现错误, 具体信息${e.getMessage}")
        }
      case _: TimestampTypeTimeColInfo =>
        throw new SparkException("数据为long类型，但您输入的时间列信息为timestamp类型")
    }
  }

  def parse(time: Timestamp): BinningTime = {
    timeColInfo match {
      case _: StringTypeTimeColInfo =>
        throw new SparkException("数据为Timestamp类型，但您输入的时间列信息为string类型")
      case _: LongTypeTimeColInfo =>
        throw new SparkException("数据为Timestamp类型，但您输入的时间列信息为long类型")
      case _: TimestampTypeTimeColInfo =>
        try {
          new BinningTime(new AbsTimeMeta(time.getTime))
        } catch {
          case e: Exception =>
            throw new SparkException(s"在将数据Timestamp格式的${time}转为时间格式时出现错误, 具体信息${e.getMessage}")
        }
    }
  }


}

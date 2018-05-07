package com.self.core.timeBinning.models

import java.sql.Timestamp
import org.apache.spark.sql.{NullableFunctions, UserDefinedFunction}
import org.joda.time
import org.joda.time.DateTime

object Tools extends Serializable {
  def binningByFixedLength(width: Long, startTimeStamp: Long)
  : UserDefinedFunction =
    NullableFunctions.udf(
      (timeStamp: Long) => (timeStamp - startTimeStamp) / width * width + startTimeStamp
    )


  /**
    * 通过自然时间分箱
    *
    * @param naturalUnit   自然时间分箱的单位 --可选择["year", "month", "season", "week", "day",
    *                      "hour", "minute", "second"]
    * @param fieldFormat   字段类型 --包括["TimeStamp", "UTC", "StringType"], 其中UTC是精确到秒的
    * @param presentFormat 展示形式 --只有当fieldFormat == "StringType"才需要
    *                      有以下方式 --["absoluteTime", "correspondingAccuracy"]
    * @param timeFormat    时间字符串的样式
    * @param utcFormat     UTC显示时精确单位。 显然timeFormat和utcFormat不能同时起效用
    * @return 返回一个udf函数
    */
  //  def binningByNaturalTime(naturalUnit: String, fieldFormat: String, presentFormat: String,
  //                           timeFormat: String = "yyyy-MM-dd HH:mm:ss", utcFormat: String = "second")
  //  : UserDefinedFunction =
  //    fieldFormat match {
  //      case "TimeStamp" => NullableFunctions.udf((timeStamp: Long) => {
  //        val dt = slidingByNatural(timeStamp, naturalUnit)
  //        new Timestamp(dt)
  //      })
  //
  //      case "UTC" => NullableFunctions.udf((timeStamp: Long) =>
  //        utcFormat match {
  //          case "second" => slidingByNatural(timeStamp, naturalUnit) / 1000L
  //          case "millisecond" => slidingByNatural(timeStamp, naturalUnit)
  //        })
  //
  //      case "StringType" => NullableFunctions.udf((timeStamp: Long) => {
  //        val dateTime = slidingByNatural(timeStamp, naturalUnit)
  //        val dt: DateTime = new time.DateTime(dateTime)
  //        presentFormat match {
  //          // 以分箱点的绝对时间显示
  //          case "absoluteTime" => dt.toString(timeFormat)
  //          // 以分箱点的时间精确到对应精度显示
  //          case "correspondingAccuracy" => naturalUnit match {
  //            case "year" => dt.toString("yyyy")
  //            case "season" => dt.toString("yyyy-MM")
  //            case "month" => dt.toString("yyyy-MM")
  //            case "week" => dt.toString("yyyy-MM-dd")
  //            case "hour" => dt.toString("yyyy-MM-dd HH")
  //            case "minute" => dt.toString("yyyy-MM-dd HH:mm")
  //            case "minute" => dt.toString("yyyy-MM-dd HH:mm:ss")
  //          }
  //        }
  //      })
  //    }


  /**
    * @param naturalUnit
    * @param format
    * @return 返回的是精确到秒的Long类型数据 是每个箱子的起始时间
    */
  def slidingByNatural(naturalUnit: String, format: String = "second"): UserDefinedFunction =
    NullableFunctions.udf((timeStamp: Long) => {
      val dateTime = format match {
        case "second" => new DateTime(timeStamp * 1000)
        case "millisecond" => new DateTime(timeStamp)
      }

      val dt: DateTime = naturalUnit match {
        case "year" => dateTime.yearOfCentury().roundFloorCopy()
        case "season" => {
          val floorMonth = dateTime.monthOfYear().roundFloorCopy()
          val month = floorMonth.monthOfYear().get()
          floorMonth.plusMonths(-(month - 1) % 3)
        }
        case "month" => dateTime.monthOfYear().roundFloorCopy()
        case "week" => dateTime.weekOfWeekyear().roundFloorCopy()
        case "day" => dateTime.dayOfYear().roundFloorCopy()
        case "hour" => dateTime.hourOfDay().roundFloorCopy()
        case "minute" => dateTime.minuteOfDay().roundFloorCopy()
        case "second" => dateTime.secondOfDay().roundFloorCopy()
      }

      dt.getMillis / 1000
    })

}

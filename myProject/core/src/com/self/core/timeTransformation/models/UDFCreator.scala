package com.self.core.timeTransformation.models

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{NullableFunctions, UserDefinedFunction}
import org.apache.spark.sql.types.{DataType, LongType, StringType, TimestampType}


/** 创建一个时间转换的udf */
object UDFCreator {
  def createUDF(
                 inputTimeFieldType: DataType,
                 UTCUnit: Option[String],
                 stringParser: Option[String => Timestamp] = None,
                 performanceInfo: (String, String)
               ): UserDefinedFunction = {
    inputTimeFieldType match {
      /** 时间字符串 */
      case StringType =>
        val parser = stringParser.get
        performanceInfo match {
          case ("string", timeFormat) =>
            NullableFunctions.udfDealWithOption {
              time: String =>
                val date = parser(time)
                val performanceTimeFormat = new SimpleDateFormat(timeFormat)
                if (date != null) {
                  Some(performanceTimeFormat.format(date))
                } else
                  None
            }

          case ("Timestamp", "") =>
            NullableFunctions.udfDealWithOption {
              time: String =>
                val date = parser(time)
                if (date != null) {
                  Some(new Timestamp(date.getTime))
                } else
                  None
            }

          case ("UTC", unit) =>
            NullableFunctions.udfDealWithOption {
              time: String =>
                val date = parser(time)
                if (date != null) {
                  Some(if (unit == "second") date.getTime / 1000L else date.getTime)
                } else
                  None
            }


        }

      case LongType =>
        val inputUTCUnit = UTCUnit.get
        performanceInfo match {
          case ("string", timeFormat) =>
            NullableFunctions.udfDealWithOption {
              time: Long =>
                val date = new Date(if (inputUTCUnit == "second") time * 1000L else time)
                val performanceTimeFormat = new SimpleDateFormat(timeFormat)
                Some(performanceTimeFormat.format(date))
            }

          case ("Timestamp", "") =>
            NullableFunctions.udfDealWithOption {
              time: Long =>
                Some(new Timestamp(if (inputUTCUnit == "second") time * 1000L else time))
            }

          case ("UTC", unit) =>
            NullableFunctions.udfDealWithOption {
              time: Long =>
                if (unit == "second" && inputUTCUnit != "second") {
                  Some(time / 1000L)
                } else if (unit != "second" && inputUTCUnit == "second") {
                  Some(time * 1000L)
                } else {
                  Some(time)
                }
            }
        }

      case TimestampType =>
        performanceInfo match {
          case ("string", timeFormat) =>
            NullableFunctions.udfDealWithOption {
              time: Timestamp =>
                val performanceTimeFormat = new SimpleDateFormat(timeFormat)
                Some(performanceTimeFormat.format(time.getTime))
            }

          case ("Timestamp", "") =>
            NullableFunctions.udfDealWithOption {
              time: Timestamp =>
                Some(time)
            }

          case ("UTC", unit) =>
            NullableFunctions.udfDealWithOption {
              time: Timestamp =>
                Some(if (unit == "second") time.getTime / 1000L else time.getTime)
            }
        }
    }

  }


}

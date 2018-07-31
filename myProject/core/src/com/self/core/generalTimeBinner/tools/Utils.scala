package com.self.core.generalTimeBinning.tools

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

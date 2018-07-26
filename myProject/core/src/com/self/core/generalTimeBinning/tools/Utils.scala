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
    while (unitCollection.hasNext && !findAGirl) {
      val nextUnit = unitCollection.next()
      if (window > unitMatch(nextUnit)) {
        timeFormat = adaptiveTimeFormat4Unit(nextUnit)
        findAGirl = true
      }
    }
    timeFormat
  }


}

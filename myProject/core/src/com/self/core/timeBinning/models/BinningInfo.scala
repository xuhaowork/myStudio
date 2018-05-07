package com.self.core.timeBinning.models

/**
  * 分箱信息
  */
trait BinningInfo

/**
  * 分为三种
  * ----
  * 1）固定长度 需要有参数length
  * 2）自然时间单位分箱一级分箱  需要有分箱的单位
  * 3）自燃时间单位分箱二级分箱  需要有一级分箱单位和二级分箱单位
  */
class FixedLengthInfo(val length: Long, val startTimeStamp: Long) extends BinningInfo
class NaturalOneStepInfo(val slidingUnit: String) extends BinningInfo
class NaturalTwoStepInfo(val slidingUnit1: String, val slidingUnit2: String) extends BinningInfo {
  val toInt = (s: String) => s match {
    case "year" => 0
    case "season" => 1
    case "month" => 2
    case "week" => 3
    case "day" => 4
    case "hour" => 5
    case "minute" => 6
    case "second" => 7
  }

  require(toInt(slidingUnit1) < toInt(slidingUnit2),
    "error201，参数错误：您输入的二级分箱中单位一的级别应该高于单位二的级别")
}


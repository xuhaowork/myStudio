package com.self.core.generalTimeBinning.models



/**
  * 分箱器
  * ----
  * 将分箱时间进行分箱，输出分箱时间
  *
  * 按星期、季度分箱时相位不发挥作用
  * 相对时间的时候无法按星期分箱，无法按季度分箱
  */


//class TimeBinner(val timeBinnerInfo: TimeBinnerInfo) extends Serializable {
//  def binning(binningTime: BinningTime) = {
//    val time = binningTime.getTipNode(timeBinnerInfo.left)
//    /** 分箱 */
//    timeBinnerInfo match {
//      case byLth: TimeBinnerInfoByLength =>
//        binningTime.binningByLength(byLth)
//      case _: TimeBinnerInfoByUnit =>
//        throw new Exception("暂时只支持等宽分箱")
//    }
//
//    /** 将分箱结果分别装在左右结点中 */
//
//
//
//  }
//
//
//}

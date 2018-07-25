package com.self.core.generalTimeBinning.models

/**
  * 分箱信息
  * ----
  *
  * @param format 分箱模式
  *               包括：byLength和byUnit两种取值
  */
abstract class TimeBinnerInfo(format: String) extends Serializable {

}


class
package com.self.core.generalTimeBinning.models

/**
  * 分箱信息
  * ----
  *
  * @param phase  一个初始相位
  * @param window 分箱窗宽
  * @param left   如果是非叶节点的话, 对BinningTime的左节点分箱还是右结点分箱
  */
class TimeBinnerInfo(
                      val phase: Long,
                      val window: Long,
                      val left: Boolean = true
                    ) extends Serializable {

}


/**
  * 通过时间长度分箱
  *
  * @param phase  一个初始相位
  * @param window 窗宽
  */
class TimeBinnerInfoByLength(
                              override val phase: Long,
                              override val window: Long,
                              override val left: Boolean = true
                            ) extends TimeBinnerInfo(phase, window, left) {


}


/**
  * 通过时间单位分箱
  *
  * @param phase  一个初始相位
  * @param window 窗宽
  * @param unit   单位
  *               year
  *               month
  *               week
  *               day
  *               hour
  *               minute
  *               second
  *               millisecond
  */
class TimeBinnerInfoByUnit(
                            override val phase: Long,
                            override val window: Long,
                            val unit: String,
                            override val left: Boolean
                          ) extends TimeBinnerInfo(phase, window, left) {


}


package com.self.core.generalTimeBinning.models

import org.joda.time.DateTime

/**
  * 分箱信息
  * ----
  *
  * @param phase  一个初始相位
  * @param window 分箱窗宽
  * @param unit   单位
  *               year
  *               month
  *               week
  *               day
  *               hour
  *               minute
  *               second
  *               millisecond
  * @param left   如果是非叶节点的话, 对BinningTime的左节点分箱还是右结点分箱
  */
class TimeBinnerInfo(
                      val phase: DateTime,
                      val window: Long,
                      val unit: String,
                      val left: Boolean
                    ) extends Serializable {


}


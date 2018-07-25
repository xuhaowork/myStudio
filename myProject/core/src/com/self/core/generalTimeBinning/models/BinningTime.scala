package com.self.core.generalTimeBinning.models

/**
  * 分箱时间
  * ----
  * 用于存储分箱信息的分箱时间
  * ----
  * 是个树状结构
  *
  * @param element    元素
  * @param leftChild  左子节点  --是取整（分箱后）的结果
  * @param rightChild 右子节点  --是取模后的结果
  * @param isLeaf     是否是叶节点，如果是叶节点则没有左子节点也没有右子节点，同时指针没有意义
  * @param point      指针，指示当前的分箱的指向（因为左右结点只活跃一个）
  */
private[generalTimeBinning] class BinningTime(
                                               val element: TimeMeta,
                                               val leftChild: Option[BinningTime],
                                               val rightChild: Option[BinningTime],
                                               val isLeaf: Boolean,
                                               val point: Byte) extends Serializable {
}


/** 用于处理时间数据的基本时间单元 */
private[generalTimeBinning] abstract class TimeMeta(val value: Long, val rightBound: Option[Long]) extends Serializable {


}

private[generalTimeBinning] class AbsTimeMeta(override val value: Long, override val rightBound: Option[Long])
  extends TimeMeta(value, rightBound) {
  def this(value: Long) = this(value, None)

  override def toString: String = ""


}

private[generalTimeBinning] class RelativeMeta(override val value: Long, override val rightBound: Option[Long])
  extends TimeMeta(value, rightBound) {
  def this(value: Long) = this(value, None)

  override def toString: String = ""

}

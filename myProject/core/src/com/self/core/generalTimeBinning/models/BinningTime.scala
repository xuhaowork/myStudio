package com.self.core.generalTimeBinning.models

import com.self.core.generalTimeBinning.tools.Utils
import org.joda.time._

/**
  * 分箱时间
  * ----
  * 功能:
  * 用于存储分箱信息的分箱时间
  * 树状结构
  * 每次分箱操作只能对叶节点进行分箱
  * 分箱前是叶节点，分箱后是非叶节点
  * ----
  * 规则：
  * 1)叶节点的左右子节点都是None，非叶节点的左右子节点都不是None
  * 2)一个非叶节点的两个子节点最多只有一个为非叶子节点(不能两个同时分箱)
  *
  * ----
  * 算法推演：
  * 1)时间记录"2017-10-10 11:23:45"
  * BinningTime(TimeMeta("2017-10-10 11:23:45"), None, None, true, 0, 0) // 非叶节点
  * // toString: 原数据:{"2017-10-10 11:23:45"}
  *
  * 2)数据分箱得到["2017-10-10 10:00:00", "01:23:45"]
  * BinningTime(TimeMeta("2017-10-10 11:23:45"), left, right, false, 0, 0) // 非叶节点
  * val left = BinningTime(TimeMeta("2017-10-10 10:00:00"), None, None, true, 0, 1, -1) // 叶节点
  * val right = BinningTime(TimeMeta("01:23:45"), None, None, true, 1, 1) // 叶节点
  * // toString: 原数据:{"2017-10-10 11:23:45"},第1次分箱:{"2017-10-10 10:00:00", "01:23:45"}
  *
  * 3)二级分箱, 指定对右节点分箱(此时point生效)
  * BinningTime(TimeMeta("2017-10-10 11:23:45"), left, right, false, 0, 0) // 非叶节点
  * val left = BinningTime(TimeMeta("2017-10-10 10:00:00"), None, None, true, 1, -1) // 叶节点
  * val right = BinningTime(TimeMeta("01:23:45"), right_left, right_right, false, 1, 1) // 非叶节点
  * val right_left = BinningTime(TimeMeta("01:00:00"), None, None, true, 2, -1) // 叶节点
  * val right_right = BinningTime(TimeMeta("00:23:45"), None, None, true, 2, 1) // 叶节点
  * // toString: 原数据:{"2017-10-10 11:23:45"},第1次分箱结果:{"2017-10-10 10:00:00"},第1次分箱剩余:"01:23:45"},
  * 第2次分箱结果:{"01:00:00"},第2次分箱剩余:"00:23:45"}
  *
  * @param element    原值
  * @param leftChild  左子节点  --是取整（分箱后）的结果
  * @param rightChild 右子节点  --是取模后的结果
  * @param isLeaf     是否是叶节点，如果是叶节点则没有左子节点也没有右子节点，同时指针没有意义
  * @param deep       深度
  * @param position   结点是左节点、右结点还是根节点的一个标志 -1代表是父节点的左节点，0代表是原数据，1代表是右结点
  */

// @todo 或许树形结构有点冗余，因为最多只有一个子结点为非叶节点，装饰着模式或许更适合，后面有时间改一下
private[generalTimeBinning] class BinningTime(
                                               var element: TimeMeta, // 不变
                                               var leftChild: Option[BinningTime], // 会变
                                               var rightChild: Option[BinningTime],
                                               var isLeaf: Boolean,
                                               var deep: Int,
                                               var position: Int = 0
                                             ) extends Serializable {
  /**
    * 构建一个新的BinningTime
    *
    * @param element 构建BinningTime用的时间
    */
  def this(element: TimeMeta) = this(element, None, None, true, 0, 0)


  def deepCopy: BinningTime = {
    new BinningTime(
      this.element, // 不变
      this.leftChild, // 会变
      this.rightChild,
      this.isLeaf,
      this.deep: Int,
      this.position)
  }


  def setLeftChild(child: BinningTime): this.type = {
    this.leftChild = Some(child)
    child.setPosition(-1)
    this.isLeaf = false
    this
  }

  def setRightChild(child: BinningTime): this.type = {
    this.rightChild = Some(child)
    child.setPosition(1)
    this.isLeaf = false
    this
  }

  def setPosition(position: Int): this.type = {
    this.position = position
    this
  }


  /** 获得最深末梢的左或右节点 --当getLeftNode为true时是左结点，大于0时是右结点 */
  def getTipNode(getLeftNode: Boolean): BinningTime =
    if (this.isLeaf) {
      this
    } else if (!this.leftChild.get.isLeaf) {
      this.leftChild.get.getTipNode(getLeftNode)
    } else if (!this.rightChild.get.isLeaf) {
      this.rightChild.get.getTipNode(getLeftNode)
    } else {
      if (getLeftNode) {
        this.leftChild.get
      } else {
        this.rightChild.get
      }
    }


  private def getString: String =
    if (deep == 0) {
      s"原数据:{${element.toString}" + "}"
    } else if (position < 0) {
      s"第${deep}级分箱结果:{${element.toString}}"
    } else {
      s"第${deep}级分箱剩余:{${element.toString}}"
    }

  /** 增加显示方式，不同级别的分箱以','分隔，每个级别的分箱展示形式为"第[[deep]]级分箱:{分箱的值}" */
  override def toString: String =
    if (this.isLeaf) {
      this.getString
    } else {
      this.getString + ", " + this.leftChild.get.toString + ", " + this.rightChild.get.toString
    }


  /** 按时间长度分箱，一个分箱时间一个剩余时间 */
  private def binningByLength(timeBinnerInfo: TimeBinnerInfoByLength): this.type = {
    val tipNode = getTipNode(timeBinnerInfo.left)

    val phase = timeBinnerInfo.phase // 相位
    val window = timeBinnerInfo.window // 时长

    val newValue = scala.math.floor((tipNode.element.value - phase) / window.toDouble).toLong * window + phase

    val binningResult = tipNode.element.update(newValue, Some(newValue + window)) // 分箱时间，沿用之前的timeFormat
    val residueResult = tipNode.element.value - newValue
    val adaptFormat4Residue = Utils.adaptTimeFormat(window)
    val residue = new RelativeMeta(residueResult, None, adaptFormat4Residue) // 剩余时间

    /** 将分箱结果分别装在左右结点中 */
    tipNode.setLeftChild(new BinningTime(binningResult, None, None, true, tipNode.deep + 1, -1))
    tipNode.setRightChild(new BinningTime(residue, None, None, true, tipNode.deep + 1, 1))
    this
  }


  /** 按时间长度分箱，一个分箱时间一个剩余时间 */
  private def binningByUnit(timeBinnerInfo: TimeBinnerInfoByUnit): this.type = {
    val tipNode = getTipNode(timeBinnerInfo.left)

    val phase = timeBinnerInfo.phase // 相位
    val window = timeBinnerInfo.window // 时长
    val unit = timeBinnerInfo.unit

    val phase_dt = new DateTime(phase).withZone(DateTimeZone.forID("UTC"))
    val value_dt = new DateTime(tipNode.element).withZone(DateTimeZone.forID("UTC"))

    val (new_value_dt, right_bound_dt) = unit match {
      case "year" =>
        val period = new Period(value_dt, phase_dt, PeriodType.years())
        (value_dt.plusYears((period.getYears / window * window.toInt).toInt),
          value_dt.plusYears((period.getYears / window * window.toInt).toInt + window.toInt))
      case "month" =>
        val period = new Period(value_dt, phase_dt, PeriodType.months())
        (value_dt.plusMonths((period.getMonths / window * window.toInt).toInt),
          value_dt.plusMonths((period.getMonths / window * window.toInt).toInt + window.toInt))
      case "week" =>
        val period = new Period(value_dt, phase_dt, PeriodType.weeks())
        (value_dt.plusWeeks((period.getWeeks / window * window.toInt).toInt),
          value_dt.plusWeeks((period.getWeeks / window * window.toInt).toInt + window.toInt))
      case "day" =>
        val period = new Period(value_dt, phase_dt, PeriodType.days())
        (value_dt.plusDays((period.getDays / window * window.toInt).toInt),
          value_dt.plusDays((period.getDays / window * window.toInt).toInt + window.toInt))
      case "hour" =>
        val period = new Period(value_dt, phase_dt, PeriodType.hours())
        (value_dt.plusHours((period.getHours / window * window.toInt).toInt),
          value_dt.plusHours((period.getHours / window * window.toInt).toInt + window.toInt))
      case "minute" =>
        val period = new Period(value_dt, phase_dt, PeriodType.minutes())
        (value_dt.plusMinutes((period.getMinutes / window * window.toInt).toInt),
          value_dt.plusMinutes((period.getMinutes / window * window.toInt).toInt + window.toInt))
      case "second" =>
        val period = new Period(value_dt, phase_dt, PeriodType.seconds())
        (value_dt.plusSeconds((period.getSeconds / window * window.toInt).toInt),
          value_dt.plusSeconds((period.getSeconds / window * window.toInt).toInt + window.toInt))
      case "millisecond" =>
        val period = new Period(value_dt, phase_dt, PeriodType.millis())
        (value_dt.plusMillis((period.getMillis / window * window.toInt).toInt),
          value_dt.plusMillis((period.getMillis / window * window.toInt).toInt + window.toInt))
      case _ =>
        throw new Exception("您输入的时间单位有误: 目前只能是year/month/week/hour/minute/second/millisecond之一")
    }

    val binningResult = tipNode.element.update(new_value_dt.getMillis, Some(right_bound_dt.getMillis))
    val relativeMillis = new Period(value_dt, new_value_dt, PeriodType.millis()).getMillis

    val adaptFormat4Residue = Utils.adaptTimeFormat(relativeMillis)
    val residue = new RelativeMeta(relativeMillis, None, adaptFormat4Residue) // 剩余时间

    /** 将分箱结果分别装在左右结点中 */
    tipNode.setLeftChild(new BinningTime(binningResult, None, None, true, tipNode.deep + 1, -1))
    tipNode.setRightChild(new BinningTime(residue, None, None, true, tipNode.deep + 1, 1))
    this
  }

  /** 分箱 */
  def binning(timeBinnerInfo: TimeBinnerInfo): this.type = {
    timeBinnerInfo match {
      case byLth: TimeBinnerInfoByLength =>
        binningByLength(byLth)
        this
      case byUnit: TimeBinnerInfoByUnit =>
        binningByUnit(byUnit)
        this
    }
  }

}


/**
  * 用于处理时间数据的基本时间单元
  * ----
  *
  * @param value      分箱值（round值），如果没有分箱之前该值就是时间的原始值
  * @param rightBound 如果是分箱后的值，该值代表分箱的右边界（不包括该值），如果是分箱前，该值为None
  * @param timeFormat 时间格式，用于后面的展示，也可以用于记忆初始的时间格式。
  */
private[generalTimeBinning] abstract class TimeMeta(
                                                     val value: Long,
                                                     val rightBound: Option[Long],
                                                     val timeFormat: String)
  extends Serializable {
  override def toString: String = {
    var str = new DateTime(value).toString(timeFormat)
    if (rightBound.isDefined)
      str += "," + new DateTime(rightBound.get).toString(timeFormat)
    "[" + str + "]"
  }

  def update(value: Long, rightBound: Option[Long]): TimeMeta

}

/** 绝对时间 */
private[generalTimeBinning] class AbsTimeMeta(
                                               override val value: Long,
                                               override val rightBound: Option[Long],
                                               override val timeFormat: String = "yyyy-MM-dd HH:mm:ss"
                                             )
  extends TimeMeta(value, rightBound, timeFormat) {
  def this(value: Long, timeFormat: String) = this(value, None, timeFormat)

  def this(value: Long) = this(value, None)

  override def update(value: Long, rightBound: Option[Long]): TimeMeta =
    new AbsTimeMeta(value, rightBound)
}

/** 相对时间 */
private[generalTimeBinning] class RelativeMeta(
                                                override val value: Long,
                                                override val rightBound: Option[Long],
                                                override val timeFormat: String
                                              ) // 相对时间统一映射到UTC时区的UTC时间
  extends TimeMeta(value, rightBound, timeFormat) {
  require(!(timeFormat.toLowerCase() contains 'e'),
    "相对时间不能以星期显示，因为时间单位星期还有绝对时间属性（例如星期一是和严格的日期对应的）")

  def this(value: Long, timeFormat: String) = this(value, None, timeFormat)

  override def update(value: Long, rightBound: Option[Long]): TimeMeta =
    new RelativeMeta(value, rightBound, timeFormat)

  override def toString: String = {
    var str = new DateTime(value).withZone(DateTimeZone.forID("UTC")).toString(timeFormat)
    if (rightBound.isDefined)
      str += "," + new DateTime(rightBound.get).withZone(DateTimeZone.forID("UTC")).toString(timeFormat)
    "[" + str + "]"
  }

}

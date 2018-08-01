package com.self.core.generalTimeBinner.models

import com.self.core.generalTimeBinner.tools.Utils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BaseGenericInternalRow, GenericInternalRow, GenericMutableRow}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
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
  * @param element        原值
  * @param leftChild      左子节点  --是取整（分箱后）的结果
  * @param rightChild     右子节点  --是取模后的结果
  * @param isLeaf         是否是叶节点，如果是叶节点则没有左子节点也没有右子节点，同时指针没有意义
  * @param deep           深度
  * @param position       结点是左节点、右结点还是根节点的一个标志 -1代表是父节点的左节点，0代表是原数据，1代表是右结点
  * @param parentPosition 是父节点的position，根节点为0，同时其两个子节点也为0
  */

// @todo 或许树形结构有点冗余，因为最多只有一个子结点为非叶节点，装饰着模式或许更适合，后面有时间改一下
@SQLUserDefinedType(udt = classOf[BinningTimeUDT])
class BinningTime(
                   var element: TimeMeta, // 不变
                   var leftChild: Option[BinningTime], // 会变
                   var rightChild: Option[BinningTime],
                   var isLeaf: Boolean,
                   var deep: Int,
                   var position: Int = 0,
                   var parentPosition: Int = 0
                 ) extends Serializable {
  /**
    * 构建一个新的BinningTime
    *
    * @param element 构建BinningTime用的时间
    */
  def this(element: TimeMeta) = this(element, None, None, true, 0, 0, 0)


  def deepCopy: BinningTime = {
    new BinningTime(
      this.element, // 不变
      this.leftChild, // 会变
      this.rightChild,
      this.isLeaf,
      this.deep: Int,
      this.position,
      this.parentPosition
    )
  }

  def setParentPosition(position: Int): this.type = {
    this.parentPosition = position
    this
  }


  def setLeftChild(child: BinningTime): this.type = {
    this.leftChild = Some(child)
    child.setPosition(-1)
    child.setParentPosition(position) // 记录下父节点的位置
    this.isLeaf = false
    this
  }

  def setRightChild(child: BinningTime): this.type = {
    this.rightChild = Some(child)
    child.setPosition(1)
    child.setParentPosition(position) // 记录下父节点的位置
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


  private def castInfoToTuple: (TimeMeta, Int, Int, Int) = {
    (element, deep, position, parentPosition)
  }

  /** element, deep, position, parentPosition */
  def castAllInfoToArray: Array[(TimeMeta, Int, Int, Int)] =
    if (isLeaf) {
      Array(castInfoToTuple)
    } else if (!leftChild.get.isLeaf) {
      val arr = leftChild.get.castAllInfoToArray
      arr ++ Array(rightChild.get.castInfoToTuple, castInfoToTuple)
    } else if (!rightChild.get.isLeaf) {
      val arr = rightChild.get.castAllInfoToArray
      arr ++ Array(leftChild.get.castInfoToTuple, castInfoToTuple)
    } else {
      Array(castInfoToTuple) ++ leftChild.get.castAllInfoToArray ++ rightChild.get.castAllInfoToArray
    }


  /** 分箱函数 */
  def binning(timeBinnerInfo: TimeBinnerInfo): this.type = {
    /** 获得当前分箱活跃的顶端叶节点 */
    val tipNode = getTipNode(timeBinnerInfo.left)

    val phase: DateTime = timeBinnerInfo.phase // 相位
    val window = timeBinnerInfo.window // 窗宽
    val unit = timeBinnerInfo.unit // 窗宽单位

    /** 先将相位和时间都以Long类型时间的数值变为默认时区下[[DateTime]]类型的时间 */
    val phase_dt = phase // 默认时区即为相位所在时区 --如果是绝对时间相位为默认时区，否则相位需要为UTC时间
    val value_dt = new DateTime(tipNode.element.value).withZone(phase.getZone)

    /** 分别获的分箱时间和分箱时间的有边界 */
    val (new_value_dt, rightBound_value_dt) = unit match {
      case "year" =>
        val period = new Period(phase_dt, value_dt, PeriodType.years())
        val value = phase_dt.plusYears(
          scala.math.floor(period.getYears.toDouble / window).toInt * window.toInt
        ).yearOfCentury().roundFloorCopy()
        (value, value.plusYears(window.toInt))

      case "month" =>
        val period = new Period(phase_dt, value_dt, PeriodType.months())
        val value = phase_dt.plusMonths(
          scala.math.floor(period.getMonths.toDouble / window).toInt * window.toInt
        ).monthOfYear().roundFloorCopy()
        (value, value.plusMonths(window.toInt))

      case "week" =>
        val period = new Period(phase_dt, value_dt, PeriodType.weeks())
        val value = phase_dt.plusWeeks(
          scala.math.floor(period.getWeeks.toDouble / window).toInt * window.toInt
        ).weekOfWeekyear().roundFloorCopy()
        (value, value.plusWeeks(window.toInt))

      case "day" =>
        val period = new Period(phase_dt, value_dt, PeriodType.days())
        val value = phase_dt.plusDays(
          scala.math.floor(period.getDays.toDouble / window).toInt * window.toInt
        ).dayOfYear().roundFloorCopy()
        (value, value.plusDays(window.toInt))

      case "hour" =>
        val period = new Period(phase_dt, value_dt, PeriodType.hours())
        val value = phase_dt.plusHours(
          scala.math.floor(period.getHours.toDouble / window).toInt * window.toInt
        ).hourOfDay().roundFloorCopy()
        (value, value.plusHours(window.toInt))

      case "minute" =>
        val period = new Period(phase_dt, value_dt, PeriodType.minutes())
        val value = phase_dt.plusMinutes(
          scala.math.floor(period.getMinutes.toDouble / window).toInt * window.toInt
        ).minuteOfDay().roundFloorCopy()
        (value, value.plusMinutes(window.toInt))

      case "second" =>
        val period = new Period(phase_dt, value_dt, PeriodType.seconds())
        val value = phase_dt.plusSeconds(
          scala.math.floor(period.getSeconds.toDouble / window).toInt * window.toInt
        ).secondOfDay().roundFloorCopy()
        (value, value.plusSeconds(window.toInt))

      case "millisecond" =>
        val period = new Period(phase_dt, value_dt, PeriodType.millis())
        val value = phase_dt.plusMillis(
          scala.math.floor(period.getMillis.toDouble / window).toInt * window.toInt
        ).millisOfDay().roundFloorCopy()
        (value, value.plusMillis(window.toInt))

      case _ =>
        throw new Exception("您输入的时间单位有误: 目前只能是year/month/week/hour/minute/second/millisecond之一")
    }

    /** 生成分箱结果信息 --绝对时间和相对时间的[createNew]方法有差异 */
    val binningResult: TimeMeta = tipNode.element.createNew(
      new_value_dt.getMillis, Some(rightBound_value_dt.getMillis))

    /** 获得分箱剩余时间 --> 自适应获得剩余时间的展示方式 --> 并生成相对时间类型的分箱时间  */
    val residueResult = value_dt.getMillis - new_value_dt.getMillis
    val adaptFormat4Residue = Utils.adaptTimeFormat(Utils.unitMatch(unit) * window)
    val residue = new RelativeMeta(residueResult, None, adaptFormat4Residue)

    /** 将分箱结果分别装在左右结点中 */
    tipNode.setLeftChild(new BinningTime(binningResult, None, None, true, tipNode.deep + 1, -1))
    tipNode.setRightChild(new BinningTime(residue, None, None, true, tipNode.deep + 1, 1))
    this
  }


  /**
    * 获取分箱结果，并分箱结果展示为对应模式的字符串
    *
    * @param format      时间字符串模式
    * @param performType interval或left或right
    * @return
    */
  def getBinningResult(format: Option[String], performType: String): String = {
    val tipNode = getTipNode(true) // 获取末端结点
    val meta = tipNode.element
    val leftBound = new DateTime(meta.value).toString(format.getOrElse(meta.timeFormat))
    performType match {
      case "left" =>
        leftBound
      case "interval" =>
        if (meta.rightBound.isDefined)
          leftBound + "," + new DateTime(meta.rightBound.get).toString(format.getOrElse(meta.timeFormat))
        else
          leftBound
      case "right" =>
        if (meta.rightBound.isDefined)
          new DateTime(meta.rightBound.get).toString(format.getOrElse(meta.timeFormat))
        else
          ""
    }
  }

  def getBinningResidue(performType: String): String = {
    val tipNode = getTipNode(false) // 获取末端结点
    val meta = tipNode.element
    val leftBound = new DateTime(meta.value).toString(meta.timeFormat)
    performType match {
      case "left" =>
        leftBound
      case "interval" =>
        if (meta.rightBound.isDefined)
          leftBound + "," + new DateTime(meta.rightBound.get).toString(meta.timeFormat)
        else
          leftBound
      case "right" =>
        if (meta.rightBound.isDefined)
          new DateTime(meta.rightBound.get).toString(meta.timeFormat)
        else
          throw new Exception("没有右边界信息")
    }

  }

}


class BinningTimeUDT extends UserDefinedType[BinningTime] {
  override def sqlType: DataType = ArrayType(
    StructType(Seq(
      StructField("element",
        StructType(Seq(
          StructField("type", ByteType, nullable = false),
          StructField("value", LongType, nullable = false),
          StructField("rightBound", LongType, nullable = true), // 可以为空
          StructField("timeFormat", ArrayType(IntegerType, containsNull = false), nullable = false)
        ))
        , false),
      StructField("deep", IntegerType, false),
      StructField("position", IntegerType, false),
      StructField("parentPosition", IntegerType, false))
    )
  )


  override def serialize(obj: Any): GenericArrayData = obj match {
    case bt: BinningTime =>
      val arr: Array[(TimeMeta, Int, Int, Int)] = bt.castAllInfoToArray
      new GenericArrayData(arr.map {
        case (element, deep, position, parentPosition) =>
          val row = new GenericMutableRow(4)
          val elObj = Utils.serializeForTimeMeta(element)
          row.update(0, elObj)
          row.setInt(1, deep)
          row.update(2, position)
          row.update(3, parentPosition)
          row
      })
    case _ =>
      throw new Exception("您输入类型不是TimeMeta在spark sql中没有预定义的序列化方法")
  }

  override def deserialize(datum: Any): BinningTime = {
    datum match {
      case arr: GenericArrayData =>
        val values = arr.array
          .map(_.asInstanceOf[BaseGenericInternalRow])
        val arrV = values.map {
          row =>
            val internalRow = row.getStruct(0, 4)
            val element = Utils.deserializeForTimeMeta(internalRow)
//            deep, position, parentPosition
            val deep = row.getInt(1)
            val position = row.getInt(2)
            val parentPosition = row.getInt(3)
            (element, deep, position, parentPosition)
        }
        Utils.reConstruct(arrV)
    }
  }

  override def pyUDT: String = "pyspark.mllib.linalg.BinningTimeUDT"

  override def userClass: Class[BinningTime] = classOf[BinningTime]

  /** 改写equal方法，使类型判别能够实现 */
  override def equals(o: Any): Boolean = {
    o match {
      case _: BinningTimeUDT => true
      case _ => false
    }
  }

  /** 改写HashCode方法使得一些调用该类型的hash方法能够以常数形式输出 */
  override def hashCode(): Int = classOf[BinningTimeUDT].getName.hashCode()

  override def typeName: String = "binningUDT"

  override def asNullable: BinningTimeUDT = this
}


/**
  * 用于处理时间数据的基本时间单元
  * ----
  *
  * @param value      分箱值（round值），如果没有分箱之前该值就是时间的原始值
  * @param rightBound 如果是分箱后的值，该值代表分箱的右边界（不包括该值），如果是分箱前，该值为None
  * @param timeFormat 时间格式，用于后面的展示，也可以用于记忆初始的时间格式。
  * @see 该类型对应一个SQL的[[UserDefinedType]] --[[TimeMetaUDT]]
  */
@SQLUserDefinedType(udt = classOf[TimeMetaUDT])
private[generalTimeBinner] abstract class TimeMeta(
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

  def createNew(value: Long, rightBound: Option[Long]): TimeMeta

}

/** 绝对时间 */
private[generalTimeBinner] class AbsTimeMeta(
                                              override val value: Long,
                                              override val rightBound: Option[Long],
                                              override val timeFormat: String = "yyyy-MM-dd HH:mm:ss"
                                            )
  extends TimeMeta(value, rightBound, timeFormat) {
  def this(value: Long, timeFormat: String) = this(value, None, timeFormat)

  def this(value: Long) = this(value, None)

  override def createNew(value: Long, rightBound: Option[Long]): TimeMeta =
    new AbsTimeMeta(value, rightBound)
}

/** 相对时间 */
private[generalTimeBinner] class RelativeMeta(
                                               override val value: Long,
                                               override val rightBound: Option[Long],
                                               override val timeFormat: String
                                             ) // 相对时间统一映射到UTC时区的UTC时间
  extends TimeMeta(value, rightBound, timeFormat) {
  require(!(timeFormat.toLowerCase() contains 'e'),
    "相对时间不能以星期显示，因为时间单位星期还有绝对时间属性（例如星期一是和严格的日期对应的）")

  def this(value: Long, timeFormat: String) = this(value, None, timeFormat)

  override def createNew(value: Long, rightBound: Option[Long]): TimeMeta =
    new RelativeMeta(value, rightBound, timeFormat)

  override def toString: String = {
    val dt = new DateTime(value).withZone(DateTimeZone.UTC)
    var str = new DateTime(value).withZone(DateTimeZone.UTC).toString(timeFormat)
    if (rightBound.isDefined)
      str += "," + new DateTime(rightBound.get).withZone(DateTimeZone.UTC).toString(timeFormat)
    "[" + str + "]"
  }

}


class TimeMetaUDT extends UserDefinedType[TimeMeta] {
  override def sqlType: DataType = StructType(Seq(
    StructField("type", ByteType, nullable = false),
    StructField("value", LongType, nullable = false),
    StructField("rightBound", LongType, nullable = true), // 可以为空
    StructField("timeFormat", ArrayType(IntegerType, containsNull = false), nullable = false)
  ))

  override def serialize(obj: Any): InternalRow = Utils.serializeForTimeMeta(obj)

  override def deserialize(datum: Any): TimeMeta = Utils.deserializeForTimeMeta(datum)

  override def pyUDT: String = "pyspark.mllib.linalg.BinningUDT"

  override def userClass: Class[TimeMeta] = classOf[TimeMeta]

  /** 改写equal方法，使类型判别能够实现 */
  override def equals(o: Any): Boolean = {
    o match {
      case _: TimeMetaUDT => true
      case _ => false
    }
  }

  /** 改写HashCode方法使得一些调用该类型的hash方法能够以常数形式输出 */
  override def hashCode(): Int = classOf[TimeMetaUDT].getName.hashCode()

  override def typeName: String = "timemeta"

  override def asNullable: TimeMetaUDT = this
}
